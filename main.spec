# -*- mode: python ; coding: utf-8 -*-

a1 = Analysis(
    ['src\\initial_pop_scanner.py'],
    pathex=[],
    binaries=[],
    datas=[('config.ini', '.initial_pop_scanner')],
    hiddenimports=['cryptography.hazmat.primitives.kdf.pbkdf2'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz1 = PYZ(a1.pure)

exe1 = EXE(
    pyz1,
    a1.scripts,
    [],
    exclude_binaries=True,
    name='initial_pop_scanner',
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
coll1 = COLLECT(
    exe1,
    a1.binaries,
    a1.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='initial_pop_scanner',
    icon='app_icon.png'
)

a2 = Analysis(
    ['src\\initial_flush_scanner.py'],
    pathex=[],
    binaries=[],
    datas=[('config.ini', '.initial_flush_scanner')],
    hiddenimports=['cryptography.hazmat.primitives.kdf.pbkdf2'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz2 = PYZ(a2.pure)

exe2 = EXE(
    pyz2,
    a2.scripts,
    [],
    exclude_binaries=True,
    name='initial_flush_scanner',
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
coll2 = COLLECT(
    exe2,
    a2.binaries,
    a2.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='initial_flush_scanner',
    icon='app_icon.png'
)



import shutil
import os

# Define the source and destination paths
source = 'config.ini'
dest_initial_pop_scanner = os.path.join('dist', 'initial_pop_scanner', 'config.ini')
dest_initial_flush_scanner = os.path.join('dist', 'initial_flush_scanner', 'config.ini')

# Copy the config.ini file to each destination
shutil.copyfile(source, dest_initial_pop_scanner)
shutil.copyfile(source, dest_initial_flush_scanner)

print(f"Copied {source} to {dest_initial_pop_scanner}")
print(f"Copied {source} to {dest_initial_flush_scanner}")