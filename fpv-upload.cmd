@echo off
if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" "%~dp0fpv_upload.py" %*
  exit /b %errorlevel%
)
where py >nul 2>nul
if %errorlevel%==0 (
  py -3 "%~dp0fpv_upload.py" %*
) else (
  python "%~dp0fpv_upload.py" %*
)
