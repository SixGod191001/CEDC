@echo off
set basepath=%~dp0
set PYTHONPATH="%PYTHONPATH%;%basepath%/../..;%basepath%/..;%basepath%/../module;%basepath%/../utils"

echo "basepath is ""%basepath%
cd /d %basepath%/..
echo "cd to parent folder "%cd%
IF EXIST ".venv" (
    echo "Found virtual environment at %cd%\.venv"
    call .\.venv\Scripts\activate
) ELSE IF EXIST "venv" (
    echo "Found virtual environment at %cd%\venv"
    call .\venv\Scripts\activate
) ELSE (
    cd /d ..
    echo "cd to parent folder CEDC"
    IF EXIST ".venv" (
    echo "Found virtual environment at CEDC\.venv"
    call .\.venv\Scripts\activate
) ELSE IF EXIST "venv" (
    echo "Found virtual environment at CEDC\venv"
    call .\venv\Scripts\activate
) ELSE (
    echo "Virtual environment not found."
    exit /b 1
)
)
echo 'Current path is: '%cd%
cd /d %basepath%
python %basepath%main.py
pause