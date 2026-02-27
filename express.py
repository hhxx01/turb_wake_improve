import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import yaml
import os
import re

def get_turb_status(data_turb,status_col,turb_num=0):
# 找出状态列所有整数
    column_data = data_turb.iloc[:, status_col]
    result = set()
    for value in column_data:
        # 检查是否为整数类型，平均了秒级数据后有许多非整数类型
        if isinstance(value, int):
            result.add(value)
        # 检查是否为浮点数且小数部分为0
        elif isinstance(value, float) and value.is_integer():
            result.add(int(value))
    turb_status = sorted(list(result))
    data_turb_status = {}
    for value in turb_status:
        data_turb_status[value] = data_turb[data_turb.iloc[:, status_col] == value]
    # plt.plot(figsize=(20, 12))
    # plt.scatter(data_turb_status[38].iloc[:, 3], data_turb_status[38].iloc[:, 0], label=str(16))
    for i in range(len(turb_status)):
        plt.plot(figsize=(20, 12))
        # plt.scatter(data_turb_status[turb_status[i]].iloc[:, 3], data_turb_status[turb_status[i]].iloc[:, 0], label=str(turb_status[i]))
        plt.scatter(data_turb_status[turb_status[i]].iloc[:, 3], data_turb_status[turb_status[i]].iloc[:, 0], label=str(turb_status[i]))
        plt.legend()
        plt.savefig('ws-p_' + str(turb_num + 1) + str(i) + '_new' + '.png')
    return turb_status,data_turb_status

def express_old_data(file_path, file_name):
    full_path = os.path.join(file_path, file_name)
    data = pd.read_csv(full_path, parse_dates=['timestamp'])
    # 将'timestamp'设置为索引
    data.set_index('timestamp', inplace=True)
    data_turb_filtered = {}
    for turb_num in range(1):# 20台机组
        data_turb = data.iloc[:, [turb_num*5, turb_num*5+1, turb_num*5+2, turb_num*5+3, turb_num*5+4]] # 选取该机组数据
        data_turb_filtered[turb_num] = data_turb[(data_turb.iloc[:, 4] == 0) & (data_turb.iloc[:, 3] == 5)] # 选不限功率且状态字为5的行
        # plt.scatter(data_turb_filtered.iloc[:, 0], data_turb_filtered.iloc[:, 2], label='no limit') # 风速-功率图
        # plt.legend()
        # plt.show()
        # turb_status, data_turb_status = get_turb_status(data_turb, 3) # 查看状态字意义
        # 查看限功率与不限功率的区别
        # data_turb_filtered_A = data_turb[(data_turb.iloc[:, 4] == 0)]
        # data_turb_filtered_B = data_turb[(data_turb.iloc[:, 4] != 0)]
        # plt.plot()
        # plt.scatter(data_turb_filtered_B.iloc[:, 0], data_turb_filtered_B.iloc[:, 2], label='limit')
        # plt.scatter(data_turb_filtered_A.iloc[:, 0], data_turb_filtered_A.iloc[:, 2], label='no limit')
        # plt.legend()
        # plt.savefig('ws-p_' + str(turb_num + 1) + '.png')
    return data_turb_filtered

def express_new_data(file_path, file_name):
    full_path = os.path.join(file_path, file_name)
    data = pd.read_csv(full_path, parse_dates=['timestamp'])
    data.set_index('timestamp', inplace=True)
    data_turb_filtered = {}
    for turb_num in range(1):# 20台机组
        data_turb = data.iloc[:, [turb_num*5, turb_num*5+1, turb_num*5+2, turb_num*5+3]]
        turb_status, data_turb_status = get_turb_status(data_turb, 1) # 8限功率 16正常发电
        data_turb_filtered[turb_num] = data_turb[(data_turb.iloc[:, 1] == 38) & (data_turb.iloc[:, 2] != 0)] # 选状态字为38且功率不为0的行

    return data_turb_filtered

def get_file_name(file_path):
    '''
    找出file_path目录下所有符合“年-月.csv”正则表达式的文件名，返回一个列表
    '''
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    all_entries = os.listdir(file_path)
    date_pattern = re.compile(r'^\d{4}-\d{2}\.csv$') # 正则匹配

    daily_files = []
    for f in all_entries: 
        condition1 = os.path.isfile(os.path.join(file_path, f)) # 是不是目录
        condition2 = date_pattern.match(f) # 是否符合正则表达式
        if condition1 and condition2:
            daily_files.append(f)
    daily_files.sort()
    return daily_files

def find_historical_reference(historical_data, current_time, turb_wind_speed, pre_year, torlence_days, torlence_hours, torlence_ws):
    target_time = current_time.replace(year=pre_year) 
    start_date = target_time - pd.Timedelta(days=torlence_days)
    end_date = target_time + pd.Timedelta(days=torlence_days)
    mask_time = (historical_data.index >= start_date) & (historical_data.index <= end_date)
    df_sub = historical_data[mask_time]
    t_start = (target_time - pd.Timedelta(hours=torlence_hours)).time()
    t_end = (target_time + pd.Timedelta(hours=torlence_hours)).time()
    times = df_sub.index.time
    if t_start <= t_end: # 跨午夜判断
        mask_hour = (times >= t_start) & (times <= t_end)
    else:
        mask_hour = (times >= t_start) | (times <= t_end)
    df_sub = df_sub[mask_hour]
    mask_ws = (df_sub.iloc[:, 0] >= turb_wind_speed - torlence_ws) & (df_sub.iloc[:, 0] <= turb_wind_speed + torlence_ws)
    final_candidates = df_sub[mask_ws]
    return final_candidates

def power_up_ws(min_ws, max_ws, slip_num, data, turb_num, is_2024 = True):
    turb_num_str = str(turb_num + 1)
    bins = np.arange(min_ws, max_ws+slip_num, int(slip_num))
    data = data.copy()
    data[turb_num_str + '_ws_bin'] = pd.cut(data[turb_num_str + '_wind_speed'], bins=bins, labels=bins[:-1]) # 风速按区间归类，labels=bins[:-1]表示左端点
    def non_zero_mean(x):
        filtered = x[x != 0]
        if filtered.empty:
            return np.nan 
        return filtered.mean()
    non_zero_std = lambda x: x[x != 0].std()
    bin_results = data.groupby(turb_num_str + '_ws_bin', observed=False).agg({
        turb_num_str + '_power': ['mean', 'count', 'std'],
        'ref_avg_power_24_' + turb_num_str: [non_zero_mean, non_zero_std],
        'ref_avg_power_23_' + turb_num_str: [non_zero_mean, non_zero_std],
        }).reset_index()
    bin_results.columns = [
        turb_num_str + '_ws_bin', 
        turb_num_str + '_avg_power', turb_num_str + '_data_count', turb_num_str + '_power_std',
        turb_num_str + '_ref_avg_power_24_mean', turb_num_str + '_ref_avg_power_24_std',
        turb_num_str + '_ref_avg_power_23_mean', turb_num_str + '_ref_avg_power_23_std'
        ]

    return bin_results


file_path_old = r'F:\data\scada\data_old\result_splited'
file_name = '2023-01.csv'
data_turbs_filtered_2023 = express_old_data(file_path_old, file_name)
file_name = '2024-01.csv'
data_turbs_filtered_2024 = express_old_data(file_path_old, file_name)
file_path_new = r'F:\data\scada\huadian_radar_realdata\result_min'
file_name = '2025-01.csv'
data_turbs_filtered_2025 = express_new_data(file_path_new, file_name)

all_turbines_list = []
power_up_list = []
for turb_num in range(1):
    data_turb_single_filtered_2025 = data_turbs_filtered_2025[turb_num]
    data_turb_single_filtered_2024 = data_turbs_filtered_2024[turb_num]
    data_turb_single_filtered_2023 = data_turbs_filtered_2023[turb_num]
    counts_2024 = []
    avg_powers_2024 = []
    counts_2023 = []
    avg_powers_2023 = []
    for index, row in data_turb_single_filtered_2025.iterrows():
        current_time = pd.to_datetime(index)
        turb_power = row.iloc[0]
        turb_wind_speed = row.iloc[3]
        final_candidates = find_historical_reference(data_turb_single_filtered_2024, current_time, turb_wind_speed, 2024, 3, 1, 0.5)
        count = len(final_candidates)
        avg_p = final_candidates.iloc[:, 2].mean() if count > 0 else 0
        counts_2024.append(count)
        avg_powers_2024.append(avg_p)
        final_candidates = find_historical_reference(data_turb_single_filtered_2023, current_time, turb_wind_speed, 2023, 3, 1, 0.5)
        count = len(final_candidates)
        avg_p = final_candidates.iloc[:, 2].mean() if count > 0 else 0
        counts_2023.append(count)
        avg_powers_2023.append(avg_p)

    data_turb_single_filtered_2025['ref_count_24_' + str(turb_num + 1)] = counts_2024
    data_turb_single_filtered_2025['ref_avg_power_24_' + str(turb_num + 1)] = avg_powers_2024
    data_turb_single_filtered_2025['ref_count_23_' + str(turb_num + 1)] = counts_2023
    data_turb_single_filtered_2025['ref_avg_power_23_' + str(turb_num + 1)] = avg_powers_2023
    all_turbines_list.append(data_turb_single_filtered_2025.copy())

    # 计算提升量：
    # 风速段提升量
    power_up_list.append(power_up_ws(0, 25, 1, data_turb_single_filtered_2025, turb_num))
final_turb_data = pd.concat(all_turbines_list, axis=0)
final_turb_data.to_csv('2025-01_all_turbines_combined.csv', encoding='utf-8-sig')

final_power_up = pd.concat(power_up_list, axis=1)
final_power_up.to_csv('2025-01_power_up_ws_bins.csv', encoding='utf-8-sig',sep=',')



# with open('config.yaml', 'r') as f:
#     config = yaml.safe_load(f)

# # 获取每月数据
# all_entries = os.listdir(config['data']['splited_data_old'])
# date_pattern = re.compile(r'^\d{4}-\d{2}\.csv$') # 正则匹配

# daily_files = []
# for f in all_entries: 
#     condition1 = os.path.isfile(os.path.join(config['data']['splited_data_old'], f)) # 是不是目录
#     condition2 = date_pattern.match(f) # 是否符合正则表达式
#     if condition1 and condition2:
#         daily_files.append(f)
# daily_files.sort()

# print(daily_files)


# for file_name in daily_files:
#     a = 1
