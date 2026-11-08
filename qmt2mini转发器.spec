# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['qmt2mini转发器.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['qmt2mini', 'qmt2mini.xtdata', 'qmt2mini.xttrader', 'qmt2mini.xttype', 'qmt2mini.xtconstant'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='qmt2mini转发器',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
