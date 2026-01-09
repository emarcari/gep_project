from datetime import datetime, date


def parse_yyyymmdd(value: str) -> date:
    """
    Parse a date string in YYYYmmdd format into a datetime.date object.

    :param value: Date string in YYYYmmdd format (e.g. 20250101)
    :return: datetime.date
    :raises ValueError: if the format is invalid
    """
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Expected format: YYYYmmdd") from exc
