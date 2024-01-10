import logging
import os
from itertools import cycle
from typing import List, Tuple, Literal

import matplotlib
import pandas as pd
from matplotlib import pyplot as plt

from tools.logging_utils import log_set

matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

DTYPE_SPEC = {"id": int, "prodName": str, "prodCatid": "Int64", "prodCat": "string", "prodPcatid": "Int64",
              "prodPcat": "string", "lowPrice": float, "highPrice": float, "avgPrice": float, "place": "string",
              "specInfo": "string", "unitInfo": str, "status": "Int64"}

LINE_STYLES = cycle(['-', '--', '-.'])


class EMDrawer(object):
    def __init__(self, filepath: str):
        self.df = self._load(filepath=filepath, dtype_spec=DTYPE_SPEC)

    @staticmethod
    def _load(filepath: str, encoding: str = "utf-8", sep: str = ",", date_format: str = "pubDate",
              dtype_spec: dict = None):
        """
        加载并处理CSV文件。

        读取指定路径的CSV文件，解析日期字段，并根据提供的数据类型字典进行类型转换。

        Parameters:
            filepath (str): CSV文件的路径
            encoding (str): 文件编码，默认为 'utf-8'
            sep (str): 字段分隔符，默认为','
            date_format (str): 日期字段的名称，默认为 'pubDate'
            dtype_spec (dict, optional): 指定每列数据的类型

        Returns:
            pd.DataFrame: 处理后的数据帧。

        Raises:
            FileNotFoundError: 如果文件路径不存在。
            ValueError: 如果数据处理过程中出现错误。
        """
        assert os.path.exists(filepath), FileNotFoundError(f"{filepath} not found")
        logging.info(f"Start Loading {filepath}")
        return pd.read_csv(filepath, sep=sep, encoding=encoding, parse_dates=[date_format], dtype=dtype_spec)

    @staticmethod
    def _filter_outliers(data, plot_values: List[str], prod_names: List[str],
                         outlier_threshold: float = 1.5) -> pd.DataFrame:
        """
        使用IQR方法过滤数据集中的异常值。

        对指定的产品名称和列应用IQR（四分位数范围）方法，以过滤掉极端的数据点。

        Parameters:
            data (pd.DataFrame): 包含数据的DataFrame。
            plot_values (List[str]): 需要过滤的列名列表。
            prod_names (List[str]): 需要过滤的产品名称列表。
            outlier_threshold (float): IQR过滤的阈值，默认为1.5。

        Returns:
            pd.DataFrame: 过滤后的数据帧。
        """
        for col in plot_values:
            for prod_name in prod_names:
                q1 = data[col][prod_name].quantile(0.25)
                q3 = data[col][prod_name].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - (outlier_threshold * iqr)
                upper_bound = q3 + (outlier_threshold * iqr)
                data.loc[data[col][prod_name].notnull(), (col, prod_name)] = data[col][prod_name].clip(
                    lower=lower_bound, upper=upper_bound)
        return data

    def preprocess(self, grouper_freq: str = 'W'):
        """
        对数据进行预处理。

        包括设置索引，按指定频率分组，并计算平均价格。支持按日、周、月进行分组。

        Parameters:
            grouper_freq (str): 分组的频率。'D' 代表按天，'W' 代表按周，'M' 代表按月。默认为'W'。

        Returns:
            None
        """
        logging.info(f"Start Pre-process, resample freq: {grouper_freq}")
        self.df.set_index(['prodCat', 'prodName', 'pubDate'], inplace=True)
        self.df = self.df.groupby(['prodCat', 'prodName', pd.Grouper(level='pubDate', freq=grouper_freq)]).agg(
            {'lowPrice': 'mean', 'highPrice': 'mean', 'avgPrice': 'mean'})

    def filter_data(self, start_date: str = None, end_date: str = None, places: List[str] = None,
                    categories: List[str] = None, product_names: List[str] = None,
                    min_price: float = None, max_price: float = None, spec_infos: List[str] = None):
        """
        对数据集应用多条件筛选。

        根据提供的日期范围、地点、产品类别、产品名称、价格范围和规格信息，筛选数据集中的记录。

        Parameters:
            start_date (str, optional): 筛选的开始日期。
            end_date (str, optional): 筛选的结束日期。
            places (List[str], optional): 筛选的地点列表。
            categories (List[str], optional): 要筛选的产品类别列表。
            product_names (List[str], optional): 要筛选的产品名称列表。
            min_price (float, optional): 最低价格筛选。
            max_price (float, optional): 最高价格筛选。
            spec_infos (List[str], optional): 要筛选的规格信息列表。

        Returns:
            None
        """
        if start_date is not None:
            self.df = self.df[self.df['pubDate'] >= pd.to_datetime(start_date)]
        if end_date is not None:
            self.df = self.df[self.df['pubDate'] <= pd.to_datetime(end_date)]
        if places is not None:
            # 使用lambda函数检查每行的place值是否包含在places列表中的任一元素内
            self.df = self.df[self.df['place'].apply(lambda x: any(place in x for place in places))]
        if categories is not None:
            self.df = self.df[self.df['prodCat'].isin(categories)]
        if product_names is not None:
            self.df = self.df[self.df['prodName'].isin(product_names)]
        if min_price is not None:
            self.df = self.df[self.df['avgPrice'] >= min_price]
        if max_price is not None:
            self.df = self.df[self.df['avgPrice'] <= max_price]
        if spec_infos is not None:
            self.df = self.df[self.df['specInfo'].isin(spec_infos)]

    def plot(self, prod_names: List[str], plot_values: List[str] = None, fig_size: Tuple[int, int] = (10, 6),
             x_label: str = 'Date', y_label: str = 'Price', title: str = None, save_path: str = './img.png',
             outlier_threshold: float = None, theme: Literal["black", "white"] = "white"):
        """
        绘制数据图表并保存。

        根据指定的产品名称和数据列绘制时间序列图，并将其保存到指定路径。

        Parameters:
            prod_names (List[str]): 要绘制的产品名称列表。
            plot_values (List[str], optional): 要绘制的数据列，默认为['lowPrice', 'highPrice']。
            fig_size (Tuple[int, int]): 图表的尺寸，默认为(10, 6)。
            x_label (str): X轴标签，默认为'Date'。
            y_label (str): Y轴标签，默认为'Price'。
            title (str, optional): 图表标题。
            save_path (str): 图表保存路径，默认为 './img.png'。
            outlier_threshold (float, optional): 异常值过滤阈值。
            theme (Literal["black", "white"]): 图表主题，默认为 'white'。

        Returns:
            None

        Raises:
            OSError: 如果保存路径不可写。
            ValueError: 如果绘图参数不合法。
        """
        plot_values = ['lowPrice', 'highPrice'] if plot_values is None else plot_values
        logging.info(f"Start plot Img, prod_names: {prod_names}, plot_values: {plot_values}, ")
        # get data
        selected_data = self.df.loc[pd.IndexSlice[:, prod_names, :], :]
        pivot_df = selected_data.pivot_table(index='pubDate', columns='prodName', values=plot_values)

        # Filtering outliers
        if outlier_threshold is not None:
            pivot_df = self._filter_outliers(pivot_df, plot_values, prod_names, outlier_threshold)

        # init plot
        if theme == "black":
            plt.style.use('dark_background')
        plt.figure(figsize=fig_size)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        if title is not None:
            plt.title(title)
        # plot_line
        for prod_name in prod_names:
            for plot_value in plot_values:
                plt.plot(pivot_df.index, pivot_df[plot_value][prod_name], label=f"{prod_name} - {plot_value}",
                         linestyle=next(LINE_STYLES))
        plt.legend(loc='upper right')

        # save
        logging.info(f"Finish create img, save to: {save_path}")
        if os.path.dirname(save_path) != "":
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
        if theme == "black":
            plt.savefig(save_path, bbox_inches='tight', dpi='figure', facecolor=theme)
        else:
            plt.savefig(save_path, transparent=True, bbox_inches='tight', dpi='figure')


if __name__ == '__main__':
    log_set(log_level=logging.INFO)
    em_drawer = EMDrawer(filepath="./xinfadi_price_detail.csv")
    em_drawer.filter_data(start_date="2023-01-01", end_date="2023-12-31")
    em_drawer.preprocess(grouper_freq='W')
    em_drawer.plot(
        # prod_names=["大白菜", "小白菜", "芹菜", "油菜", "香菜"],
        prod_names=["大白菜", "小白菜"],
        # plot_values=['lowPrice', 'highPrice'],
        plot_values=['avgPrice'],
        save_path="./save.png",
        outlier_threshold=1.5,
        theme="white"
    )
