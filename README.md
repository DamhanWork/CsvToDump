# CsvToDump
A small python tool to take a folder of CSVs and turn them into a SQL dump for sharing.

## Install:
- Clone the repo
- Create your virtual environment & activate it
```
python -m venv venv
source venv/bin/activate
```
- Install the requirements
```
pip install -r requirements.txt
```

## Usage
```
python main.py "path/to/required/folder" "desired output db name"
```
