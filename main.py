import argparse
from datetime import datetime, timedelta
import pytz
import pandas as pd

from src.config_values import *
from src.csv_writer import (
    write_reference_csv,
    normalize_filename_part,
    build_csv_filename,
)
from src.date_util import parse_yyyymmdd
from src.logging_util import setup_logging
from src.payload_factory import PayloadFactory
from src.powerbi_client import PowerBIClient
from src.response_parser import DailySeriesParser
from src.token_provider import EmbedTokenProvider

logger = setup_logging()


def fill_nans_with_previous(df: pd.DataFrame) -> pd.DataFrame:
    """Help method to fill nan values with previous valid values."""
    df = df.copy()
    df["value"] = df["value"].ffill()
    return df


if __name__ == "__main__":
    logger.info("Starging PowerBI extractor")

    parser = argparse.ArgumentParser(description="GEP Test extraction task")
    parser.add_argument(
        "--processing-datetime",
        required=False,
        type=str,
        help="Processing datetime, in YYYYmmdd_HHMMSS format (e.g., 20231005_143000)",
    )

    parser.add_argument(
        "--department",
        required=True,
        type=str,
        help="Department name, according to Bolsa Mercantil de Colombia (e.g., 'Nacional')",
    )

    parser.add_argument(
        "--product",
        required=True,
        type=str,
        help="Product name, according Bolsa Mercantil de Colombia (e.g., 'Azúcar Blanco')",
    )

    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_yyyymmdd,
        help="Start date, in YYYYmmdd format",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_yyyymmdd,
        help="End date, in YYYYmmdd format",
    )

    parser.add_argument(
        "--apply-fillna",
        action="store_true",
        help="Fill missing values using the previous valid values",
    )

    parser.add_argument(
        "--write-daily-values-csv",
        action="store_true",
        help="If a csv file of daily values should be written",
    )

    parser.add_argument(
        "--write-monthly-values-csv",
        action="store_true",
        help="If a csv file of values aggregated (mean) for each month should be written",
    )

    parser.add_argument(
        "--write-mean-csv",
        action="store_true",
        help="If a csv file of total mean of given period should be written",
    )

    args = parser.parse_args()
    processing_datetime = args.processing_datetime
    if processing_datetime:
        logger.info(f"Processing datetime provided: {processing_datetime}")
    else:
        processing_datetime = datetime.now(tz=pytz.UTC).strftime("%Y%m%d_%H%M%S")
        logger.info(
            f"No processing datetime provided, using current datetime: {processing_datetime}"
        )

    department_name = args.department
    product_name = args.product

    start_date = args.start_date
    end_date = args.end_date
    # make end_date inclusive
    end_date_given = args.end_date
    end_date = end_date + timedelta(days=1)

    if start_date > end_date:
        raise ValueError("start-date must be earlier than or equal to end-date")

    apply_fillna = args.apply_fillna
    write_daily_values = args.write_daily_values_csv
    write_monthly_values = args.write_monthly_values_csv
    write_mean_csv = args.write_mean_csv

    logger.info("Running extractor for given values")
    logger.info(f"department: {department_name}")
    logger.info(f"product: {product_name}")
    logger.info(f"start-date: {start_date}")
    logger.info(f"end-date: {end_date}")
    logger.info(f"apply-fillna: {apply_fillna}")

    token_provider = EmbedTokenProvider(TOKEN_URL)
    embed_token = token_provider.get_token()

    client = PowerBIClient(CLUSTER, REPORT_ID, embed_token)
    models = client.get_models_and_exploration()

    mwc_token = client.get_mwc_token(models)
    if not mwc_token:
        logger.error("Could not find mwc_token")
        raise

    payload = PayloadFactory.daily_series(
        dataset_id=DATASET_ID,
        report_id=REPORT_ID,
        visual_id=VISUAL_ID,
        start_date=start_date,
        end_date=end_date,
        product=product_name,
        department=department_name,
    )

    response = client.execute_query(QES_ENDPOINT, mwc_token, payload)

    parser = DailySeriesParser()
    rows = parser.parse(response, product_name)

    # Now we have a list of dicts in format [{'date', 'product', 'value'}]
    # Let's spend some memory and write a pandas
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if apply_fillna:
        logger.info("Applying fill na")
        df = fill_nans_with_previous(df)

    min_date = df["date"].min()
    max_date = df["date"].max()
    num_days = df["date"].nunique()

    logger.info(
        "Dataframe covers %d distinct days (%s - %s)",
        num_days,
        min_date.strftime("%Y-%m-%d"),
        max_date.strftime("%Y-%m-%d"),
    )

    base_filename = build_csv_filename(
        department=department_name,
        product=product_name,
        start_date=start_date,
        end_date=end_date_given,
        apply_fillna=apply_fillna,
    )

    if write_daily_values:
        daily_filename = base_filename + ".csv"
        logger.info(f"writing: {daily_filename}")
        write_reference_csv(df, daily_filename, logger)

    if write_monthly_values:
        df["date"] = pd.to_datetime(df["date"])
        df["year_month"] = df["date"].dt.to_period("M")

        monthly_avg = df.groupby(["product", "year_month"], as_index=False)[
            "value"
        ].mean()

        monthly_avg["date"] = monthly_avg["year_month"].dt.to_timestamp()
        monthly_avg = monthly_avg.drop(columns=["year_month"])

        monthly_filename = base_filename + "_monthly.csv"
        logger.info(f"writing: {monthly_filename}")
        write_reference_csv(monthly_avg, monthly_filename, logger)

    if write_mean_csv:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])

        result = df.groupby("product", as_index=False).agg(
            value=("value", "mean"), date=("date", "min")
        )

        mean_filename = base_filename + "_mean.csv"
        logger.info(f"writing: {mean_filename}")
        write_reference_csv(result, mean_filename, logger)

    logger.info(f"That's all folks! The work is done")
