import os
from typing import List, Tuple

import matplotlib
import pandas as pd
from matplotlib import pyplot as plt

matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

DTYPE_SPEC = {"id": int, "prodName": str, "prodCatid": "Int64", "prodCat": "string", "prodPcatid": "Int64",
              "prodPcat": "string", "lowPrice": float, "highPrice": float, "avgPrice": float, "place": "string",
              "specInfo": "string", "unitInfo": str, "status": "Int64"}


class EMDrawer(object):
    def __init__(self, filepath: str):
        self.df = self._load(filepath=filepath, dtype_spec=DTYPE_SPEC)

    @staticmethod
    def _load(filepath: str, encoding: str = "utf-8", sep: str = ",", date_format: str = "pubDate",
              dtype_spec: dict = None):
        """加载数据集，并清洗空白项"""
        assert os.path.exists(filepath), FileNotFoundError(f"{filepath} not found")
        return pd.read_csv(filepath, sep=sep, encoding=encoding, parse_dates=[date_format], dtype=dtype_spec)

    def preprocess(self, grouper_freq: str = 'W'):
        """
        数据预处理。设置索引，过滤极值，按指定频率分组，并计算平均价格
        :param grouper_freq: 分组的频率。'D' 代表按天，'W' 代表按周，'M' 代表按月
        """
        self.df.set_index(['prodCat', 'prodName', 'pubDate'], inplace=True)
        self.df = self.df.groupby(['prodCat', 'prodName', pd.Grouper(level='pubDate', freq=grouper_freq)]).agg(
            {'lowPrice': 'mean', 'highPrice': 'mean', 'avgPrice': 'mean'})

    def plot(self, prod_names: List[str], plot_values: List[str] = None, fig_size: Tuple[int, int] = (10, 6),
             x_label: str = 'Date', y_label: str = 'Price', title: str = None, save_path: str = 'img.png'):
        # get data
        selected_data = self.df.loc[pd.IndexSlice[:, prod_names, :], :]
        pivot_df = selected_data.pivot_table(index='pubDate', columns='prodName',
                                             values=['lowPrice', 'highPrice'] if plot_values is None else plot_values)

        # init plot
        plt.figure(figsize=fig_size)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        if title is not None:
            plt.title(title)
        # plot_line
        for prod_name in prod_names:
            plt.plot(pivot_df.index, pivot_df['lowPrice'][prod_name], label=f"{prod_name} - lowPrice")
            plt.plot(pivot_df.index, pivot_df['highPrice'][prod_name], label=f"{prod_name} - highPrice", linestyle='--')
        plt.legend(loc='upper right')

        # save
        if os.path.dirname(save_path) != "":
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, transparent=True, bbox_inches='tight', dpi='figure')


if __name__ == '__main__':
    em_drawer = EMDrawer(filepath="./xinfadi_price_detail.csv")
    em_drawer.preprocess(grouper_freq='2D')
    em_drawer.plot(
        prod_names=["大白菜", "娃娃菜"],
        plot_values=['lowPrice', 'highPrice'],
        save_path="save.png"
    )
