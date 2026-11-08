#coding:gbk
r"""
探针J: 文件桥原版客户端 + 定时器每秒ping(51757)
  完全复刻用户日常用法: 粘贴原版客户端 + schedule_run定时器里调ping()
  若10分钟存活 -> 文件桥姿势安全, 凶手在qmt2mini_server一侧
  若~2-3分钟死 -> 文件桥姿势已被10-27更新弄坏
"""
import datetime as dt

# -*- coding: gbk -*-
"""
QMT文件读写转换器 - 客户端（大QMT内置环境用, 纯标准库, GBK编码）

用法一（推荐, 粘贴进策略）:
    把本文件全部内容复制到大QMT策略文件的顶部(#coding:gbk之后), 然后:
        原来: f = open('D:/out/信号.csv', 'a', encoding='gbk'); f.write(line); f.close()
        现在: append_text('D:/out/信号.csv', line)          # 走TCP由服务端代写
        原来: txt = open('D:/out/池子.csv', encoding='gbk').read()
        现在: txt = read_text('D:/out/池子.csv', 'gbk')     # 走TCP由服务端代读
        或几乎不改代码: f = bridge_open('D:/out/x.csv', 'w', 'gbk'); f.write(s); f.close()

用法二（模块导入）:
    把本文件放到策略同目录, 策略里 import qmt_file_bridge_client

要求: 先在普通Python里启动服务端 qmt_file_bridge_server.py(同一台机器),
     默认 127.0.0.1:51757, 端口不一致时用 set_bridge('127.0.0.1', 端口) 修改。
连接策略: 首次用到时自动连接; 断线自动重连; 传输类错误自动重试一次。
线程安全: 内部加锁, handlebar与run_time定时器并发调用均可。
"""
import base64
import json
import os
import socket
import struct
import threading

BRIDGE_PROTOCOL_PORT = 51757       # 与服务端默认端口一致
_BRIDGE_HOST_DEFAULT = '127.0.0.1'


class QmtFileBridgeError(Exception):
    """传输/连接类错误(内部已自动重试过一次)"""


class QmtFileBridgeServerError(QmtFileBridgeError):
    """服务端返回的业务错误(如路径未放行、文件不存在), 不重试"""


class QmtFileBridge(object):
    """TCP文件桥接客户端, 单实例持有一条长连接, 加锁串行收发"""

    def __init__(self, host=_BRIDGE_HOST_DEFAULT, port=BRIDGE_PROTOCOL_PORT,
                 connect_timeout=5.0, io_timeout=120.0):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.io_timeout = io_timeout
        self._sock = None
        self._lock = threading.Lock()
        self._req_id = 0

    # ---------- 连接与帧收发 ----------

    def _connect(self):
        try:
            s = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        except Exception as e:
            raise QmtFileBridgeError('无法连接转换器服务端 %s:%s (请先启动 qmt_file_bridge_server.py): %s'
                                     % (self.host, self.port, e))
        s.settimeout(self.io_timeout)
        self._sock = s

    def _close(self):
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _recv_exact(self, n):
        buf = b''
        while len(buf) < n:
            chunk = self._sock.recv(min(65536, n - len(buf)))
            if not chunk:
                raise QmtFileBridgeError('服务端关闭了连接')
            buf += chunk
        return buf

    def _exchange(self, op, params):
        self._req_id += 1
        req = dict(params)
        req['op'] = op
        req['id'] = self._req_id
        payload = json.dumps(req, ensure_ascii=False).encode('utf-8')
        self._sock.sendall(struct.pack('>I', len(payload)) + payload)
        (n,) = struct.unpack('>I', self._recv_exact(4))
        body = self._recv_exact(n)
        resp = json.loads(body.decode('utf-8'))
        if not resp.get('ok'):
            raise QmtFileBridgeServerError('服务端错误[%s]: %s' % (op, resp.get('error', '未知')))
        return resp.get('data')

    def _request(self, op, **params):
        with self._lock:
            for attempt in (0, 1):
                try:
                    if self._sock is None:
                        self._connect()
                    return self._exchange(op, params)
                except QmtFileBridgeServerError:
                    # 业务错误: 连接仍是好的, 直接抛出, 不重试
                    raise
                except QmtFileBridgeError:
                    # 传输类错误: 重连一次再试
                    self._close()
                    if attempt == 1:
                        raise
                except Exception as e:
                    # socket超时/复位等原生异常, 同样按传输错误处理
                    self._close()
                    if attempt == 1:
                        raise QmtFileBridgeError('%s: %s' % (type(e).__name__, e))

    # ---------- 文件操作接口 ----------

    def ping(self):
        """连通性测试, 返回 {protocol, roots, ...}"""
        return self._request('ping')

    def read_text(self, path, encoding='utf-8', newline=None):
        """读文本; 默认通用换行(与内置open一致), newline=''则与磁盘字节完全一致"""
        d = self._request('read', path=path, binary=False, encoding=encoding, newline=newline)
        return d['text']

    def read_bytes(self, path):
        """读二进制, 返回bytes"""
        d = self._request('read', path=path, binary=True)
        return base64.b64decode(d['b64'])

    def write_text(self, path, text, encoding='utf-8', append=False, create_dirs=True):
        """写文本(内容按指定编码原样落盘, 不做换行翻译); append=True追加"""
        d = self._request('write', path=path, data=text, binary=False,
                          encoding=encoding, append=append, create_dirs=create_dirs)
        return d['size']

    def write_bytes(self, path, data, append=False, create_dirs=True):
        """写二进制bytes"""
        d = self._request('write', path=path, data=base64.b64encode(data).decode('ascii'),
                          binary=True, append=append, create_dirs=create_dirs)
        return d['size']

    def append_text(self, path, text, encoding='utf-8'):
        return self.write_text(path, text, encoding=encoding, append=True)

    def append_bytes(self, path, data):
        return self.write_bytes(path, data, append=True)

    def exists(self, path):
        return bool(self._request('exists', path=path))

    def file_size(self, path):
        return int(self._request('size', path=path))

    def stat(self, path):
        return self._request('stat', path=path)

    def list_dir(self, path):
        """列目录, 返回 [{name, is_dir, size, mtime}, ...]"""
        return self._request('list', path=path)

    def make_dir(self, path):
        return self._request('mkdir', path=path)

    def delete_file(self, path):
        return self._request('delete', path=path)

    def rename_file(self, src, dst):
        return self._request('rename', src=src, dst=dst)

    def close(self):
        with self._lock:
            self._close()


class BridgeFile(object):
    """仿内置open()的TCP文件对象, 方便把旧代码里的 open(...) 最小改动换成 bridge_open(...)。
    实现为整体读入/整体写回: 'r'打开时一次取回全部内容; 'w'/'a'在close/flush时整体写盘。
    支持 r/rb/w/wb/a/ab, 支持with语句; 不支持seek/随机读写。"""

    def __init__(self, bridge, path, mode='r', encoding='utf-8'):
        self.bridge = bridge
        self.path = path
        self.encoding = encoding
        self.closed = False
        self.binary = 'b' in mode
        m = mode.replace('b', '').replace('t', '').replace('+', '')
        if m not in ('r', 'w', 'a'):
            raise ValueError('暂不支持模式 %r (支持 r/rb/w/wb/a/ab)' % mode)
        self._mode = m
        self._buf = b'' if self.binary else ''
        self._pos = 0
        if m == 'r':
            if not bridge.exists(path):
                raise IOError('[Errno 2] 文件不存在: %s' % path)
            if self.binary:
                self._buf = bridge.read_bytes(path)
            else:
                self._buf = bridge.read_text(path, encoding)
        elif m == 'a':
            if bridge.exists(path):
                if self.binary:
                    self._buf = bridge.read_bytes(path)
                else:
                    self._buf = bridge.read_text(path, encoding)

    def read(self, n=-1):
        if self.closed:
            raise ValueError('文件已关闭')
        if self._mode != 'r':
            raise IOError('非读模式不可read')
        if n is None or n < 0:
            out = self._buf[self._pos:]
            self._pos = len(self._buf)
        else:
            out = self._buf[self._pos:self._pos + n]
            self._pos += len(out)
        return out

    def write(self, s):
        if self.closed:
            raise ValueError('文件已关闭')
        if self._mode == 'r':
            raise IOError('只读模式不可write')
        if self.binary:
            if not isinstance(s, bytes):
                raise TypeError('二进制模式write需要bytes')
        else:
            if not isinstance(s, str):
                raise TypeError('文本模式write需要str')
        self._buf += s
        return len(s)

    def flush(self):
        if self.closed or self._mode == 'r':
            return
        if self.binary:
            self.bridge.write_bytes(self.path, self._buf)
        else:
            self.bridge.write_text(self.path, self._buf, self.encoding)

    def close(self):
        if self.closed:
            return
        try:
            if self._mode in ('w', 'a'):
                self.flush()
        finally:
            self.closed = True

    def readable(self):
        return self._mode == 'r' and not self.closed

    def writable(self):
        return self._mode in ('w', 'a') and not self.closed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# ---------- 默认实例与模块级便捷函数 ----------

_DEFAULT_BRIDGE = QmtFileBridge()


def set_bridge(host='127.0.0.1', port=BRIDGE_PROTOCOL_PORT, **kw):
    """服务端端口/地址与默认不同时调用, 例: set_bridge('127.0.0.1', 6000)"""
    global _DEFAULT_BRIDGE
    _DEFAULT_BRIDGE = QmtFileBridge(host, port, **kw)
    return _DEFAULT_BRIDGE


def get_bridge():
    return _DEFAULT_BRIDGE


def bridge_open(path, mode='r', encoding='utf-8'):
    """内置open()的替换品, 读写全部经TCP由服务端完成"""
    return BridgeFile(_DEFAULT_BRIDGE, path, mode, encoding)


def ping():
    return _DEFAULT_BRIDGE.ping()


def read_text(path, encoding='utf-8'):
    return _DEFAULT_BRIDGE.read_text(path, encoding)


def read_bytes(path):
    return _DEFAULT_BRIDGE.read_bytes(path)


def write_text(path, text, encoding='utf-8', append=False, create_dirs=True):
    return _DEFAULT_BRIDGE.write_text(path, text, encoding=encoding, append=append,
                                      create_dirs=create_dirs)


def write_bytes(path, data, append=False, create_dirs=True):
    return _DEFAULT_BRIDGE.write_bytes(path, data, append=append, create_dirs=create_dirs)


def append_text(path, text, encoding='utf-8'):
    return _DEFAULT_BRIDGE.append_text(path, text, encoding)


def append_bytes(path, data):
    return _DEFAULT_BRIDGE.append_bytes(path, data)


def exists(path):
    return _DEFAULT_BRIDGE.exists(path)


def file_size(path):
    return _DEFAULT_BRIDGE.file_size(path)


def stat_file(path):
    """元信息: {exists, is_dir, is_file, size, mtime}"""
    return _DEFAULT_BRIDGE.stat(path)


def list_dir(path):
    return _DEFAULT_BRIDGE.list_dir(path)


def delete_file(path):
    return _DEFAULT_BRIDGE.delete_file(path)


def rename_file(src, dst):
    return _DEFAULT_BRIDGE.rename_file(src, dst)


def self_test():
    """连通性自检: 向服务端ping, 并在其放行目录下做一轮读写。成功返回True"""
    info = ping()
    roots = info.get('roots') or []
    if not roots:
        print('[自检] 服务端未放行任何目录, 无法读写测试')
        return False
    d = roots[0].rstrip('/\\') + '/_bridge_selftest'
    p = d + '/自检.txt'
    write_text(p, 'qmt bridge ok\n中文测试\n', encoding='gbk', create_dirs=True)
    back = read_text(p, encoding='gbk')
    delete_file(p)
    ok = ('中文测试' in back)
    print('[自检] 协议%s 服务端Python%s 放行目录=%s 读写=%s'
          % (info.get('protocol'), info.get('python'), roots[0], '通过' if ok else '失败'))
    return ok


if globals().get('__name__') == '__main__':
    # 用globals().get取值式判断: QMT等exec环境里__name__可能未定义, 直接写__name__会NameError;
    # 再兜一层异常: QMT里即使触发自检, 服务端没开也只是提示, 不会让策略加载失败
    try:
        self_test()
    except Exception as _e:
        print('[客户端自检失败]', _e)


_n = [0]

def _tick(C):
    _n[0] += 1
    try:
        info = ping()
        if _n[0] % 10 == 0:
            print('J[存活%d秒] ping OK protocol=%s' % (_n[0], info.get('protocol')))
    except Exception as e:
        if _n[0] % 10 == 0:
            print('J[ping失败] %s' % repr(e)[:60])


def init(C):
    print('J=== 探针J启动: 原版文件桥客户端 每秒ping 51757 ===')
    C.set_account(account)
    C.subscribe_quote('000300.SH', 'tick', 'none', 'dict', lambda *a: None)
    C.schedule_run(_tick, '20200101000000', -1, dt.timedelta(seconds=1), 'probeJ')


def handlebar(C):
    pass
