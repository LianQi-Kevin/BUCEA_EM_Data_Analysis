import asyncio
import contextlib
import csv
import logging
import math
import os
from typing import List
from urllib import parse

import aiofiles
import aiohttp

from tools.logging_utils import log_set
from tools.pydantic_types import XinFaDiPriceDetailRequest, XinFaDiPriceDetailResponse

PRICE_DETAIL_API = "http://www.xinfadi.com.cn/getPriceData.html"

PROXIES = {
    "http": None,
    "https": None,
    "socks5": None
}
TIMEOUT = aiohttp.ClientTimeout(total=60)


def urlencode_with_no_none(data):
    """移除值为None的键值对"""
    cleaned_data = {k: v for k, v in data.items() if v is not None}
    return parse.urlencode(cleaned_data)


async def xinfadi_price(data: XinFaDiPriceDetailRequest = None, retry_count: int = 10):
    """异步请求获取新发地价格数据"""
    if retry_count <= 0:
        logging.error("Maximum retry attempts reached. Failed to Get XinFaDi Price Detail.")
        raise RuntimeError("Failed to Get XinFaDi Price Detail.")
    try:
        async with aiohttp.request(
                method='POST',
                url=PRICE_DETAIL_API,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
                },
                data=parse.urlencode({"limit": 20, "current": 1}) if data is None else urlencode_with_no_none(
                    data.model_dump(by_alias=True)),
                timeout=TIMEOUT,
                proxy=PROXIES['http']
        ) as response:
            # 记录响应状态码和头信息
            logging.debug(f"Response Status: {response.status}")
            logging.debug(f"Response Headers: {response.headers}")

            # 异步读取并记录响应体
            response_body = await response.text()  # 或者使用 response.json() 对于JSON响应
            logging.debug(f"Response Body: {response_body}")

            # 转换为json格式并传入pydantic进行数据格式化
            response.raise_for_status()
            json_response = await response.json()
            logging.debug(f"Get XinFaDi Price Detail successful, data: {json_response}")
            return XinFaDiPriceDetailResponse(**json_response)
    except aiohttp.ClientError as e:
        logging.warning(f"aiohttp request error, retry count: {retry_count}, error: {e}")
        await asyncio.sleep(3)
        return await xinfadi_price(data, retry_count - 1)
    except Exception as e:
        logging.warning(f"Get XinFaDi Price Detail failed, retry count: {retry_count}, error: {e}")
        await asyncio.sleep(3)
        return await xinfadi_price(data, retry_count - 1)


async def write_csv(export_path: str, queue, headers: List[str] = None, flush_every: int = 400):
    async with aiofiles.open(export_path, mode='w', encoding='utf-8', newline='') as file:
        logging.debug(f"Event loop: {asyncio.get_running_loop()}")
        writer = None
        item_index = 0

        while True:
            if writer is None:
                # 首次写入，创建writer并写入表头
                writer = csv.DictWriter(file, fieldnames=headers)
                await writer.writeheader()

            data = await queue.get()
            if data is None:
                # None作为结束信号
                break
            await writer.writerow(data)
            item_index += 1
            queue.task_done()

            if item_index % flush_every == 0:
                await file.flush()
                os.fsync(file.fileno())


async def task_main(index, data: XinFaDiPriceDetailRequest, queue, semaphore=None, delay: float = 0):
    await asyncio.sleep(delay)
    async with semaphore if semaphore else contextlib.nullcontext():
        logging.debug(f"Event loop: {asyncio.get_running_loop()}")
        logging.info(f"Start task {index}")
        try:
            logging.debug(f"data: {data.model_dump(by_alias=True)}")
            response = await xinfadi_price(data, retry_count=100)
            logging.debug(f"task index: {index}, Successful get data: {response}")
            for item in response.list:
                await queue.put(item.model_dump(by_alias=True))
            logging.debug(f"task index: {index}, Successful add to writer queue")
            # return response
        except RuntimeError:
            logging.error(f"Task {index} failed, data: {data.model_dump(by_alias=True)}")
            await asyncio.sleep(3)
            return None


async def main(single_limit: int = 40, export_path: str = "xinfadi_price_detail.csv"):
    # 获取对象总数
    single_response = await xinfadi_price(XinFaDiPriceDetailRequest(**{"limit": 1, "current": 1}))
    loop_num = math.ceil(single_response.count / single_limit)

    # 创建队列
    queue = asyncio.Queue()

    # 写入表头
    csv_headers = list(single_response.list[0].model_dump(by_alias=True).keys())
    writer_task = asyncio.create_task(write_csv(export_path=export_path, queue=queue, headers=csv_headers))

    # 控制异步任务数量
    semaphore = asyncio.Semaphore(100)

    # 创建异步任务
    group_size = 100  # 每组协程的数量
    tasks = [
        task_main(
            index=index,
            data=XinFaDiPriceDetailRequest(**{"limit": single_limit, "current": index + 1}),
            semaphore=semaphore,
            delay=index // group_size,
            queue=queue
        )
        for index in range(0, loop_num)
    ]

    # 等待所有任务完成
    await asyncio.gather(*tasks)

    await queue.put(None)  # 发送None结束写入任务
    await writer_task


if __name__ == '__main__':
    log_set(log_level=logging.DEBUG, log_save=True, save_level=logging.INFO)
    asyncio.run(main())
