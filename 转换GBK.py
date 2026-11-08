# -*- coding: utf-8 -*-
"""
转换GBK.py — 把 *_src.py（UTF-8编辑源码）转换为可粘贴进大QMT的GBK成品

QMT策略引擎要求策略文件为 GBK 编码（首行 #coding:gbk）。仓库里的 *_src.py
是 UTF-8 编辑源码，直接粘贴会乱码——改完源码先跑本脚本再粘贴：

用法: py -3.14 转换GBK.py 桥_QMT端_src.py [输出文件.py]
默认输出: 同名去掉 _src（桥_QMT端_src.py -> 桥_QMT端.py），已存在则需确认覆盖
"""
import os
import sys


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    src = sys.argv[1]
    if not os.path.exists(src):
        print('找不到文件:', src)
        sys.exit(1)
    dst = sys.argv[2] if len(sys.argv) > 2 else src.replace('_src.py', '.py')
    if dst == src:
        print('输出名与源相同，请显式指定输出文件')
        sys.exit(1)
    text = open(src, encoding='utf-8').read()
    try:
        data = text.encode('gbk')
    except UnicodeEncodeError as e:
        print('源码含GBK无法编码的字符:', e)
        sys.exit(1)
    if os.path.exists(dst):
        if input('%s 已存在，覆盖? [y/N] ' % dst).strip().lower() != 'y':
            print('已取消')
            sys.exit(0)
    with open(dst, 'wb') as f:
        f.write(data)
    print('已转换: %s -> %s (%d 字节, GBK)' % (src, dst, len(data)))


if __name__ == '__main__':
    main()
