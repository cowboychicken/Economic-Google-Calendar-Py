# Define the path to the Python script
$pythonScriptPath = ".\src\economic_calendar.py"

echo "Changing directory and running update..."

# Working directory
cd C:\Users\user\Documents\Programming_Projects\economiccalendar-py

pipenv run python $pythonScriptPath

echo "Update done...sleep(10)..."

# Sleep for 10 seconds
Start-Sleep -Seconds 10