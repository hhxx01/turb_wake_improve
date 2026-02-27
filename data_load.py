import numpy as np
import pandas as pd
import yaml
import os
import re

# 导入历史数据,去除不需要的数据
    
def load_historical_data(file_path, year, month):
    file_path = file_path + str(year) + '_' + str(month).zfill(2) + '.csv'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    data = pd.read_csv(file_path)
    return data



# 历史数据拆分
def split_data_by_month(file_path, file_name):
    full_path = os.path.join(file_path, file_name)
    data = pd.read_csv(full_path, encoding='gbk')
    data['timestamp'] = pd.to_datetime(data['时间'])
    # 将'timestamp'设置为索引
    data.set_index('timestamp', inplace=True)
    data = data.drop(columns=['时间'])
    for month, group in data.groupby(pd.Grouper(freq='M')):
        # 生成文件名，例如：2018-01.csv
        file_name_month = f"{month.strftime('%Y-%m')}.csv"
        # 将每个月的数据写入对应的CSV文件
        full_path = os.path.join(file_path, file_name_month)
        group.to_csv(full_path, encoding='utf-8-sig')


# 现有数据合并 秒级数据--分钟级，各个机组数据拆分
def load_now_data(file_path, file_name):
    full_path = os.path.join(file_path, file_name)
    data = pd.read_csv(full_path, usecols=[0, 43, 44, 45, 47, 48, 50])
    data.set_index(data.columns[0], inplace=True)
    data.index.name = 'timestamp' # 转索引
    data.index = pd.to_datetime(data.index) # 转时间格式
    print('loaded', file_name)
    return data

def split_now_data_minute(file_path, data, save_path):
    # 选取data每一分钟的数据，根据num列平均并重排
    grouped = data.groupby(['num', pd.Grouper(freq='1min')]).mean() # 根据机组id和分钟级时间重新分组后进行平均
    # 使用 unstack(level=0) 将 'num' 这一层索引转到“列”上
    wide_df = grouped.unstack(level=0)
    wide_df = wide_df.swaplevel(0, 1, axis=1).sort_index(axis=1) # 调换列索引顺序并排序,否则会先输出所有机组功率
    wide_df.columns = [f"{num}_{col}" for num, col in wide_df.columns] # 重命名列，加上机组名
    wide_df.index.name = 'timestamp'
    month_str = data.index[0].strftime('%Y-%m')
    save_path = os.path.join(save_path, f"{month_str}.csv")
    file_exists = os.path.isfile(save_path)
    wide_df.to_csv(save_path, mode='a', header=not file_exists, encoding='utf-8-sig')

def split_now_data_second(file_path, data):
    # 注意data index索引已经设置为时间格式
    wide_df = data.pivot(columns='num') # 使用num列透视，将num列转为多列
    wide_df = wide_df.swaplevel(0, 1, axis=1).sort_index(axis=1) # 重排
    wide_df.columns = [f"{num}_{col}" for num, col in wide_df.columns]
    wide_df.index.name = 'timestamp'
    month_str = data.index[0].strftime('%Y-%m')
    save_path = os.path.join(file_path,'result_sec', f"{month_str}.csv")
    file_exists = os.path.isfile(save_path)
    wide_df.to_csv(save_path, mode='a', header=not file_exists, encoding='utf-8-sig')


with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# split_data_by_month(config['data']['raw_path_old'], '历史数据.csv')

all_entries = os.listdir(config['data']['raw_path_new'])
date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}\.csv$') # 正则匹配

daily_files = []
for f in all_entries: 
    condition1 = os.path.isfile(os.path.join(config['data']['raw_path_new'], f)) # 是不是目录
    condition2 = date_pattern.match(f) # 是否符合正则表达式
    if condition1 and condition2:
        daily_files.append(f)
daily_files.sort()

for file_name in daily_files:
    data = load_now_data(config['data']['raw_path_new'], file_name)
    split_now_data_minute(config['data']['raw_path_new'], data, config['data']['splited_data_new'])
    # split_now_data_second(config['data']['raw_path_new'], data)

