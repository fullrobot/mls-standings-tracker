from prefect import flow

from etl_raw import extract, transform, load
from etl_analytics import create_analytics_tables


@flow(name="ETL Main Flow", description="Main ETL flow for MLS data processing")
def main_etl_flow():
    data = extract()
    transformed_df = transform(data)
    load(transformed_df)
    create_analytics_tables()
    print("ETL process completed successfully.")


if __name__ == "__main__":
    main_etl_flow()
    print("ETL flow executed successfully.")
