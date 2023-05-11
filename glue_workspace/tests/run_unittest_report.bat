@echo off
set basepath=%~dp0
cd /d %basepath%/..
IF EXIST ".venv" (
    echo "Found virtual environment at %basepath%.venv"
    call .\.venv\Scripts\activate
) ELSE IF EXIST "venv" (
    echo "Found virtual environment at %basepath%venv"
    call .\venv\Scripts\activate
) ELSE (
    cd /d ..
    IF EXIST ".venv" (
    echo "Found virtual environment"
    call .\.venv\Scripts\activate
) ELSE IF EXIST "venv" (
    echo "Found virtual environment"
    call .\venv\Scripts\activate
) ELSE (
    echo "Virtual environment not found."
    exit /b 1
)

)

echo 'Current path is: '%basepath%
python %basepath%main.py
pause

