import pandas as pd
from datetime import date
import logging
import re
import unicodedata


def write_reference_csv(
    df: pd.DataFrame,
    output_path: str,
    logger: logging.Logger | None = None,
) -> None:
    """
    Write a CSV file in the format:
    Referencia, Data, Valor

    :param df: DataFrame with columns [product, date, value]
    :param output_path: Path to the output CSV file
    :param logger: Optional logger
    """
    if logger:
        logger.info("Writing CSV to %s", output_path)

    required_columns = {"product", "date", "value"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    output_df = (
        df.rename(
            columns={
                "product": "Referencia",
                "date": "Data",
                "value": "Valor",
            }
        )
        .assign(Data=lambda x: pd.to_datetime(x["Data"]).dt.strftime("%d/%m/%Y"))
        .loc[:, ["Referencia", "Data", "Valor"]]
    )

    output_df.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
        sep=",",
    )

    if logger:
        logger.info("CSV successfully written (%d rows)", len(output_df))


def normalize_filename_part(value: str) -> str:
    """
    Normalize a string to be filesystem-safe:
    - remove accents
    - replace spaces with underscores
    - remove non-alphanumeric characters
    - lowercase
    """
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = value.replace(" ", "_")
    value = re.sub(r"[^a-z0-9_]", "", value)
    return value


def format_date_for_filename(d: date) -> str:
    """
    Format date as YYYY_MM_DD for filenames.
    """
    return d.strftime("%Y_%m_%d")


def build_csv_filename(
    department: str,
    product: str,
    start_date: date,
    end_date: date,
    apply_fillna: bool,
) -> str:
    fillna_suffix = "with_fillna" if apply_fillna else "without_fillna"

    filename = (
        f"values_"
        f"{normalize_filename_part(department)}_"
        f"{normalize_filename_part(product)}_"
        f"{format_date_for_filename(start_date)}_"
        f"{format_date_for_filename(end_date)}_"
        f"{fillna_suffix}"
    )

    return filename
