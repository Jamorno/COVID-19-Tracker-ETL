# 🦠 COVID-19 Tracker ETL Project

This project extracts COVID-19 statistics from a public API, transforms the data, loads it into a PostgreSQL database, and exports a summary CSV file.

## Features

- Extracts real-time COVID-19 data via API
- Transforms & cleans data using pandas
- Loads data into PostgreSQL using psycopg2
- Exports summary as CSV
- Structured using OOP & `.env` for secure configuration

## Tech Stack

- Python
- Pandas
- psycopg2
- PostgreSQL
- Logging
- python-dotenv

## Getting Started

```bash
# 1. Clone the repo
git clone https://github.com/Jamorno/COVID-19-Tracker-ETL.git
cd COVID-19-Tracker-ETL

# 2. Set up a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create .env file
touch .env

# Paste into .env file
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password_here

# 5. Run the ETL pipeline
python main.py
```