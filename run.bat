@echo off
setlocal
cd /d "%~dp0"

set "PYTHON=C:\Users\Vitor\AppData\Local\Programs\Python\Python312\python.exe"
set "PY=C:\Users\Vitor\AppData\Local\Programs\Python\Python312\python.exe"
set "PATH=C:\Users\Vitor\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin;%PATH%"

:start
set "PYTHONPATH=%CD%"

if exist ".venv" (
    echo [INFO] Removendo .venv anterior...
    rmdir /s /q ".venv"
)

echo [INFO] Criando ambiente virtual com Python 3.12...
"%PYTHON%" -m venv .venv
if errorlevel 1 (
    echo [ERRO] Falha ao criar a .venv.
    exit /b 1
)

call ".venv\Scripts\activate.bat"

"%CD%\.venv\Scripts\python.exe" -m pip install -U pip setuptools wheel --no-input -q

if exist "requirements.txt" (
    echo [INFO] Instalando dependencias...
    "%CD%\.venv\Scripts\pip.exe" install -r requirements.txt --no-input -q
)

echo [INFO] Iniciando bot...
"%CD%\.venv\Scripts\python.exe" bot\main.py

echo [WARN] Bot finalizado. Reiniciando em 5 segundos...
ping -n 6 127.0.0.1 >nul
goto start