from covid_ETL import CovidETL

if __name__ == "__main__":
    url = "https://disease.sh/v3/covid-19/countries"
    etl = CovidETL(url)
    etl.run()