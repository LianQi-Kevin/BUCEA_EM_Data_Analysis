import asyncio
import csv
import logging
import math
from typing import List

import aiofiles
import aiohttp

from tools.logging_utils import log_set
from tools.pydantic_types import XinFaDiPriceDetailRequest, XinFaDiPriceDetailResponse

PROXIES = {
    "http": None,
    "https": None,
    "socks5": None
}

queue = asyncio.Queue()


async def xinfadi_price(session, data: XinFaDiPriceDetailRequest = None, retry_count: int = 10):
    """异步请求获取新发地价格数据"""
    if retry_count <= 0:
        logging.error("Maximum retry attempts reached. Failed to Get XinFaDi Price Detail.")
        raise RuntimeError("Failed to Get XinFaDi Price Detail.")
    try:
        async with session.post(
                "http://www.xinfadi.com.cn/getPriceData.html",
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
                },
                data={"limit": 20, "current": 1} if data is None else data.model_dump(by_alias=True),
                timeout=60
        ) as response:
            response.raise_for_status()
            json_response = await response.json()
            logging.debug(f"Get XinFaDi Price Detail successful")
            return XinFaDiPriceDetailResponse(**json_response)
    except Exception as e:
        logging.warning(f"Get XinFaDi Price Detail failed, retry count: {retry_count}, error: {e}")
        await asyncio.sleep(3)
        return await xinfadi_price(session, data, retry_count - 1)


async def write_csv(export_path: str, headers: List[str] = None):
    async with aiofiles.open(export_path, mode='w', encoding='utf-8', newline='') as file:
        writer = None
        while True:
            data = await queue.get()
            if data is None:
                # None作为结束信号
                break
            if writer is None:
                # 首次写入，创建writer并写入表头
                writer = csv.DictWriter(file, fieldnames=headers)
                await file.write(','.join(headers) + '\n')
            await writer.writerow(data)
            queue.task_done()


async def task_main(session, index, data: XinFaDiPriceDetailRequest):
    logging.info(f"Start task {index}")
    try:
        logging.debug(f"data: {data.model_dump(by_alias=True)}")
        response = await xinfadi_price(session, data)
        logging.debug(f"task index: {index}, Successful get data: {response}")
        response_data = response.list  # 假设这是获取到的数据字典
        for item in response_data:
            await queue.put(item)
        logging.debug(f"task index: {index}, Successful add to writer queue")
        # return response
    except RuntimeError:
        logging.error(f"Task {index} failed, data: {data.model_dump(by_alias=True)}")
        return None


async def main(single_limit: int = 40, export_path: str = "xinfadi_price_detail.csv"):
    async with aiohttp.ClientSession() as session:
        # 获取对象总数
        single_response = await xinfadi_price(session, XinFaDiPriceDetailRequest(**{"limit": 1, "current": 1}))
        loop_num = math.ceil(single_response.count / single_limit)

        # 写入表头
        csv_headers = list(single_response.list[0].model_dump(by_alias=True).keys())
        writer_task = asyncio.create_task(write_csv(export_path, csv_headers))

        tasks = [
            task_main(session, index, XinFaDiPriceDetailRequest(**{"limit": single_limit, "current": index + 1}))
            for index in range(loop_num)
        ]

        # 等待所有任务完成
        await asyncio.gather(*tasks)

        # 所有任务完成后，发送结束信号给写入任务
        await queue.put(None)
        await writer_task


if __name__ == '__main__':
    log_set(log_level=logging.DEBUG, log_save=True, save_level=logging.INFO)
    asyncio.run(main())
