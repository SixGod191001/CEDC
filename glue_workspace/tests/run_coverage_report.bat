@echo off
set basepath=%~dp0
cd /d %basepath%/..
call .\venv\Scripts\activate
coverage run -m unittest discover
coverage html
pause