import csv
import logging
import os

from pydantic import ValidationError

from tools.logging_utils import log_set
from tools.pydantic_types import XinFaDiPriceDetailResponseDataObject


def verify_data(csv_path: str = "./xinfadi_price_detail.csv"):
    assert os.path.exists(csv_path), FileExistsError(f"{csv_path} not found")
    with open(csv_path, "r", encoding="utf-8") as csv_f:
        for index, row in enumerate(csv.DictReader(csv_f)):
            try:
                logging.info(f"line: {index}, data: {row}")
                XinFaDiPriceDetailResponseDataObject(**row)
            except ValidationError as e:
                logging.error(f"line: {index}, pydantic error: {e}")
            except Exception as e:
                logging.critical(f"error: {e}")


if __name__ == '__main__':
    log_set(log_level=logging.WARNING)
    verify_data()
