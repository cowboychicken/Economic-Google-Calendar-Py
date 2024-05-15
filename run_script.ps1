# Define the path to the Python script
$pythonScriptPath = ".\src\economic-calendar.py"

echo "Changing directory and running update..."

# Working directory
cd C:\Users\user\Documents\Programming_Projects\economiccalendar-py

pipenv run python $pythonScriptPath


echo "Update done...zzz..."

# Sleep for 15 seconds
Start-Sleep -Seconds 15