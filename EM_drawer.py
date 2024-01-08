import os
from typing import Literal, Optional, List

import matplotlib
import pandas as pd
from matplotlib import pyplot as plt

matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

DTYPE_SPEC = {
    "id": int,
    "prodName": str,
    "prodCatid": "Int64",
    "prodCat": "string",
    "prodPcatid": "Int64",
    "prodPcat": "string",
    "lowPrice": float,
    "highPrice": float,
    "avgPrice": float,
    "place": "string",
    "specInfo": "string",
    "unitInfo": str,
    "status": "Int64"
}


class EMDrawer(object):
    def __init__(self, csv_path: str):
        self.df = self._load_csv(csv_path, dtype_spec=DTYPE_SPEC, clean="columns")

    @staticmethod
    def _load_csv(csv_path: str, encoding: str = "utf-8", sep: str = ",", date_format: str = "pubDate",
                  dtype_spec: dict = None, clean: Optional[Literal["index", "columns", "rows"]] = None):
        """加载数据集，并清洗空白项"""
        assert os.path.exists(csv_path), FileNotFoundError(f"{csv_path} not found")
        df = pd.read_csv(csv_path, sep=sep, encoding=encoding, parse_dates=[date_format], dtype=dtype_spec)
        return df if clean is None else df.dropna(axis=clean, how='all')

    @staticmethod
    def _create_combined_name(row, main_key: str, suffixes: List[str] = None):
        """创建{main_key}_{suffix0}_{suffix1}样式的名称"""
        combined_str = f"{main_key}"
        if suffixes is not None:
            for suffix in suffixes:
                if suffix in row.index and pd.notna(row[suffix]):
                    combined_str = f"{combined_str}_{row[suffix]}"
        # return combined_str.replace('\\', '').replace('/', '').replace('.', '')
        return combined_str

    def filtered_df_creator(self, filter_name: str, suffixes: List[str] = None):
        """过滤数据集"""
        # 按名称筛选
        df = self.df[self.df["prodName"] == filter_name]
        # 丢弃无用列
        df = df.drop(['id', 'prodCat', 'prodPcat', 'prodPcatid', 'prodCatid', 'status', 'lowPrice', 'highPrice'],
                     axis="columns")
        # 合并名称、产地及备注
        df['merged_name'] = df.apply(self._create_combined_name, axis="columns", args=(filter_name, suffixes))
        # 显示保留并重新排序，可省略
        # df = df[['merged_name', 'lowPrice', 'highPrice', 'avgPrice', 'unitInfo', 'pubDate']]
        df = df[['merged_name', 'avgPrice', 'unitInfo', 'pubDate']]
        return df

    @staticmethod
    def draw_plt(df, x_tag: str, y_tag: str, sub_values: List[str] = None, title: str = None, aggfunc: str = 'mean',
                 save_path: str = None):
        pivot_table = df.pivot_table(values=sub_values, index=x_tag, columns=y_tag, aggfunc=aggfunc)
        # 调整图例位置并添加自定义标题
        fig, ax = plt.subplots(figsize=(10, 6))

        for name in pivot_table.columns.levels[1]:
            for sub_value in sub_values:
                ax.plot(pivot_table.index, pivot_table[(sub_value, name)], label=f'{name} - {sub_value}')

        # 将图例移至左侧
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

        # 设置自定义标题
        ax.set_title(title)

        # 设置坐标轴标签
        ax.set_xlabel("Date")
        # ax.set_xlabel(x_tag)
        ax.set_ylabel("Price")
        # ax.set_ylabel(y_tag)

        # save
        if save_path is not None:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            plt.savefig(save_path, transparent=True, bbox_inches='tight', dpi='figure')


if __name__ == '__main__':
    em_drawer = EMDrawer(csv_path="./xinfadi_price_detail.csv")
    filtered_pd = em_drawer.filtered_df_creator(filter_name="榴莲", suffixes=["place"])
    em_drawer.draw_plt(filtered_pd, x_tag="pubDate", y_tag="merged_name", save_path='./榴莲.png',
                       sub_values=['avgPrice'], title="Product Price Trends Over Time")
    # filtered_pd = em_drawer.filtered_df_creator(filter_name="娃娃菜", suffixes=["place"])
    # em_drawer.draw_plt(filtered_pd, x_tag="pubDate", y_tag="merged_name", save_path='./WaWaCai.png',
    #                    sub_values=['avgPrice'], title="Product Price Trends Over Time")
    # em_drawer.draw_plt(filtered_pd, x_tag="pubDate", y_tag="merged_name", save_path='./WaWaCai.png',
    #                    sub_values=['lowPrice', 'highPrice', 'avgPrice'], title="Product Price Trends Over Time")
