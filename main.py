import csv
import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import requests

from tools.logging_utils import log_set
from tools.pydantic_types import XinFaDiPriceDetailRequest, XinFaDiPriceDetailResponse, \
    XinFaDiPriceDetailResponseDataObject

PROXIES = {
    "http": None,
    # "http": "http://127.0.0.1:52539",
    "https": None,
    # "https": "http://127.0.0.1:52539",
    "socks5": None
}


def xinfadi_price(data: XinFaDiPriceDetailRequest = None, retry_count: int = 10):
    """通过requests请求获取新发地价格数据"""
    if retry_count <= 0:
        logging.error("Maximum retry attempts reached. Failed to Get XinFaDi Price Detail.")
        raise RuntimeError("Failed to Get XinFaDi Price Detail.")
    try:
        response = requests.request(
            method="POST",
            url="http://www.xinfadi.com.cn/getPriceData.html",
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
            },
            proxies=PROXIES,
            data={"limit": 20, "current": 1} if data is None else data.model_dump(by_alias=True),
            timeout=60
        )
        response.raise_for_status()
        response.encoding = "UTF-8"
        logging.debug(f"Get XinFaDi Price Detail successful")
        return XinFaDiPriceDetailResponse(**response.json())
    except requests.exceptions.RequestException as e:
        logging.warning(f"Get XinFaDi Price Detail failed, retry count: {retry_count}, error: {e}")
        time.sleep(3)
        return xinfadi_price(data, retry_count - 1)


def write_csv(write_lock, data_lst: List[XinFaDiPriceDetailResponseDataObject] = None, headers: List[str] = None,
              export_path: str = "xinfadi_price_detail.csv", write_mode: str = "a"):
    with write_lock:
        logging.debug(f"Start to write {export_path}, data: {data_lst}, headers: {headers}")
        with open(export_path, write_mode, encoding="utf-8", newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            if data_lst is not None:
                for data in data_lst:
                    writer.writerow(data.model_dump(by_alias=True))


def thread_main(write_lock, index, data: XinFaDiPriceDetailRequest, csv_headers: List[str],
                export_path: str = "xinfadi_price_detail.csv"):
    logging.info(f"Start thread {index}")
    try:
        logging.debug(f"data: {data.model_dump(by_alias=True)}")
        response = xinfadi_price(data)
        logging.debug(f"thread index: {index}, Successful get data: {response}")
        write_csv(write_lock, response.list, write_mode="a", headers=csv_headers, export_path=export_path)
        logging.debug(f"thread index: {index}, Successful to write csv")
        return response
    except RuntimeError:
        logging.error(f"Thread {index} failed, data: {data.model_dump(by_alias=True)}")
        return None
    finally:
        logging.info(f"Thread {index} finished")


def main(single_limit: int = 40, export_path: str = "xinfadi_price_detail.csv"):
    write_lock = threading.Lock()
    # get object total count
    single_response = xinfadi_price(XinFaDiPriceDetailRequest(**{"limit": 1, "current": 1}))
    loop_num = math.ceil(single_response.count / single_limit)

    futures = []
    # write headers
    csv_headers = list(single_response.list[0].model_dump(by_alias=True).keys())
    with write_lock:
        with open(export_path, "w", encoding="utf-8", newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_headers)
            writer.writeheader()

    with ThreadPoolExecutor(max_workers=70) as executor:
        for index in range(loop_num):
            futures.append(
                executor.submit(
                    thread_main,
                    write_lock,
                    index,
                    XinFaDiPriceDetailRequest(**{"limit": single_limit, "current": index + 1}),
                    csv_headers,
                    "xinfadi_price_detail.csv"
                ))
            time.sleep(0.35)

        for future in as_completed(futures):
            logging.debug(future.result())
    logging.info(f"Finish all thread, total thread num: {loop_num}")


if __name__ == '__main__':
    log_set(log_level=logging.DEBUG, log_save=True, save_level=logging.INFO)
    main()
