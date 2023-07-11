# system python interpreter. used only to create virtual environment
PY = python3
VENV = venv
BIN=$(VENV)/bin

# make it work on windows too
ifeq ($(OS), Windows_NT)
    BIN=$(VENV)/Scripts
    PY=python
endif


all: lint test

$(VENV): requirements.txt setup.py
    $(PY) -m venv $(VENV)
    $(BIN)/pip install --upgrade -r requirements.txt
    $(BIN)/pip install -e .
    touch $(VENV)