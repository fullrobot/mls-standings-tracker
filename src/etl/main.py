from prefect import flow

from etl_raw import etl_flow
from etl_analytics import create_analytics_tables


@flow(name="ETL Main Flow", description="Main ETL flow for MLS data processing")
def main_etl_flow():
    for year in range(2015, 2026):
        print(f"Starting ETL process for the year: {year}")
        etl_flow(year=str(year))
        print(f"ETL process for the year {year} completed successfully.")
    print("Raw data ETL process completed successfully.")
    create_analytics_tables()
    print("ETL process completed successfully.")


if __name__ == "__main__":
    main_etl_flow()
    print("ETL flow executed successfully.")
