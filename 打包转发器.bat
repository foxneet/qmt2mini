@echo off
rem qmt2mini转发器 打包脚本 —— 产出 qmt2mini转发器.exe
rem 首次使用先安装打包器: py -3.13 -m pip install pyinstaller
cd /d %~dp0
py -3.13 -m PyInstaller --onefile --name qmt2mini转发器 --collect-submodules qmt2mini qmt2mini转发器.py
echo.
echo 产物: dist\qmt2mini转发器.exe
pause
