# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['src\\ipo_info_scraper.py'],
    pathex=[],
    binaries=[],
    datas=[('config.ini', '.')],
    hiddenimports=['cryptography.hazmat.primitives.kdf.pbkdf2'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ipo_info_scraper',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ipo_info_scraper',
    icon='app_icon.png'
)


import shutil
shutil.copyfile('config.ini', 'dist/ipo_info_scraper/config.ini')