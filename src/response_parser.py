from datetime import datetime, date
import logging


class DailySeriesParser:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def parse(self, response: dict, product: str) -> list[dict]:
        ds = response["results"][0]["result"]["data"]["dsr"]["DS"][0]
        points = ds["PH"][0]["DM0"]

        rows = []
        for p in points:
            ts = p["G0"]
            value = p.get("X", [{}])[0].get("M0")

            rows.append(
                {
                    "date": datetime.utcfromtimestamp(ts / 1000).date(),
                    "product": product,
                    "value": value,
                }
            )

        self.logger.info("Parsed %d rows", len(rows))
        return rows
