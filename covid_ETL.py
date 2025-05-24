import requests, logging, psycopg2
import pandas as pd
from config import db_config

logging.basicConfig(
    filename="covid_etl.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class CovidETL:
    def __init__(self, api_url):
        self.api_url = api_url
        self.conn = psycopg2.connect(**db_config)

    def extract_data(self):
        try:
            response = requests.get(self.api_url)
            response.raise_for_status()
            data = response.json()
            logging.info("Data extract from API.")
            return data
        except Exception as e:
            logging.error(f"Failed to fetch API: {e}")
            return None

    def transform_data(self, raw_data):
        try:
            records = []
            for data in raw_data:
                records.append(
                    {
                        "country": data.get("country"),
                        "cases": data.get("cases"),
                        "deaths": data.get("deaths"),
                        "recovered": data.get("recovered"),
                        "population": data.get("population"),
                        "updated": pd.to_datetime(data["updated"], unit="ms")
                    }
                )

            df = pd.DataFrame(records)
            logging.info(f"Transformed {len(df)} records.")
            return df

        except Exception as e:
            logging.error(f"Error transforming data: {e}")
            return pd.DataFrame()

    def load_data_to_postgres(self, df):
        try:
            cursor = self.conn.cursor()

            cursor.execute("DROP TABLE IF EXISTS covid_stats")

            cursor.execute(
                """CREATE TABLE IF NOT EXISTS covid_stats (
                country TEXT, cases INTEGER, deaths INTEGER, recovered INTEGER, 
                population INTEGER, updated TIMESTAMP);"""
            )

            for _, row in df.iterrows():
                cursor.execute(
                    """INSERT INTO covid_stats (country, cases, deaths, recovered, population, updated) 
                    VALUES (%s, %s, %s, %s, %s, %s)""",(
                        row["country"], row["cases"], row["deaths"], row["recovered"], row["population"], row["updated"]
                    )
                )

            self.conn.commit()
            logging.info("Loaded data to PostgreSQL.")

        except Exception as e:
            logging.error(f"Error to load data to PostgreSQL: {e}")

        finally:
            cursor.close()

    def export_summary(self, output_csv="covid_summary.csv"):
        try:
            query = "SELECT * FROM covid_stats"
            df = pd.read_sql(query, self.conn)
            df.to_csv(output_csv, index=False)
            logging.info(f"Export summary to {output_csv}")

        except Exception as e:
            logging.error(f"Failed to export summary: {e}")

    def run(self):
        raw = self.extract_data()
        if raw is None:
            logging.error("No data to process.")
            return

        df = self.transform_data(raw)
        if df.empty:
            logging.warning("No data after transformation.")
            return

        self.load_data_to_postgres(df)
        self.export_summary()

        self.conn.close()

        logging.info("ETL and Export data completed")
        logging.info("................................................................")