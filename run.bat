@echo off

echo Installing dependencies...
pip install -r requirements.txt

echo Running the main pipeline...
python main.py

echo Running unit tests...
python -m unittest test_main.py

pause
