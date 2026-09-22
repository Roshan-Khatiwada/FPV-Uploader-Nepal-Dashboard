@echo off
setlocal
set "SOURCE=%~1"
if "%SOURCE%"=="" set /p "SOURCE=Enter the drive or folder containing vendor sessions: "
if "%SOURCE%"=="" (
  echo No source folder was provided.
  exit /b 2
)
set /p "L1=Optional L1 (press Enter to leave blank): "
set /p "L2=Optional L2 (press Enter to leave blank): "
set /p "L3=Optional L3 (press Enter to leave blank): "
call "%~dp0fpv-upload.cmd" upload-vendor "%SOURCE%" --l1 "%L1%" --l2 "%L2%" --l3 "%L3%"
exit /b %errorlevel%
