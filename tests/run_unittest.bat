@echo off
set basepath=%~dp0
cd /d %basepath%/..
call .\venv\Scripts\activate
echo 'Current path is: '%basepath%
python %basepath%main.py
pause