@echo off

REM Activate the virtual environment
REM Replace this path with your Python 3.11 virtual environment and run
call D:\v311\Scripts\activate.bat

REM Run the Python script
python  OptionChainStreamer.py

REM Deactivate the virtual environment
deactivate