import numpy as np
import pandas as pd
import yaml
import os
from constans import TURB_NUM, TURB_ATTRIBUTES

def read_turb_data(file_path, file_name):
    full_path = os.path.join(file_path, file_name)
    data = pd.read_csv(full_path, parse_dates=['timestamp'])
    data.set_index('timestamp', inplace=True)
    # turb_data = data.iloc[:, [range(turb_num*TURB_ATTRIBUTES, (turb_num + 1)*TURB_ATTRIBUTES)]] # 选取该机组数据
    return data

def yaw_num_counter(data, turb_num):
    yaw_num = 0
    for i in range(len(data) - 2):
        if data[f'{turb_num}_position'][i] == data[f'{turb_num}_position'][i + 1] and data[f'{turb_num}_position'][i] != data[f'{turb_num}_position'][i + 2]:
            yaw_num  = yaw_num + 1
    return yaw_num

def yaw_num_counter_torlence(data, turb_num, torlence = 5):

    pos = data[f'{turb_num}_position']
    status = data[f'{turb_num}_turbine_status']
    is_steady = (np.abs(pos.shift(1) - pos) < torlence)
    is_changing = (np.abs(pos - pos.shift(-1)) > torlence)
    is_state = (np.round(status) == 38)

    return (is_steady & is_changing & is_state).sum()

def get_yaw_speed_series(data, turb_num):
    pos_col = f'{turb_num}_position'
    pos_diff = data[pos_col].diff()
    pos_diff = (pos_diff + 180) % 360 - 180  # 修正循环角度差
    time_diff = data.index.to_series().diff().dt.total_seconds()
    speed = pos_diff.abs() / time_diff
    
    # 过滤掉非数值、无穷大以及静止不动的点 
    speed = speed.replace([np.inf, -np.inf], np.nan).dropna()
    speed = speed[speed > 0.01] # 阈值可以根据传感器精度调整
    return speed


# 1.用scada计算所有机组偏航次数，看偏航次数是否确实存在异常
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)
all_entries = os.listdir(config['data']['splited_data_new'])

# results = []
# for file_name in all_entries:
#     row_data = {'file_name': file_name}
#     data = read_turb_data(config['data']['splited_data_new'], file_name)
#     for turb_num in range(1, TURB_NUM + 1):
#         # yaw_num = yaw_num_counter(data, turb_num)
#         yaw_num = yaw_num_counter_torlence(data, turb_num, torlence=5)
#         print(file_name, turb_num, yaw_num)
#         row_data[f'Turb_{turb_num}'] = yaw_num
#     results.append(row_data)
# yaw_num_df = pd.DataFrame(results)
# yaw_num_df.to_csv('./F01_yaw/F01_yaw_counter_tor.csv')

# 2. 统计偏航速率频率分布，看看是否存在异常的偏航速率
def process_yaw_speed_frequencies(results, num_bins):
    all_values = pd.concat([df.stack() for df in results]) # 合并
    global_min = np.floor(all_values.min())
    global_max = np.ceil(all_values.max())
    bins = np.linspace(global_min, global_max, num_bins + 1)

    binned_results = []
    column_labels = [f"{bins[i]:.2f}--{bins[i+1]:.2f}" for i in range(len(bins)-1)]
    for df_speed in results:
        file_freq_list = []
        for col in df_speed.columns: # 遍历 Turb_1, Turb_2...
            counts, _ = np.histogram(df_speed[col].dropna(), bins=bins)
            row = {'Turbine_ID': col}
            row.update(dict(zip(column_labels, counts)))
            file_freq_list.append(row)
        # 转换为频次统计表
        df_freq = pd.DataFrame(file_freq_list)
        binned_results.append(df_freq)
    return binned_results, bins

# results = []
# for file_name in all_entries:
#     file_dict = {}
#     data = read_turb_data(config['data']['splited_data_new'], file_name)
#     for turb_num in range(1, TURB_NUM + 1):
#         file_dict[f'Turb_{turb_num}'] = (get_yaw_speed_series(data, turb_num))
#         df_speed = pd.DataFrame(file_dict)
#     results.append(df_speed)
# num_bins = 20
# frequency_dfs, final_bins = process_yaw_speed_frequencies(results, num_bins)
# for freq_df, file_name in zip(frequency_dfs, all_entries):

#     freq_df.to_csv(f'./F01_yaw/F01_yaw_speed_{file_name}.csv', index = False)


# 3.偏航角与风向 
def get_yaw_wd_diff(data, turb_num):
    dir_diff = data[f'{turb_num}_wind_direction1'] 
    state = data[f'{turb_num}_turbine_status']
    shortest_diff = np.where(dir_diff > 180, 360 - dir_diff, dir_diff)
    # filtered_diff = shortest_diff[state == 38]
    shortest_diff[state != 38] = 0
    return np.abs(shortest_diff)


# results = []
# for file_name in all_entries:
#     file_dict = {'file_name': file_name}
#     data = read_turb_data(config['data']['splited_data_new'], file_name)
#     for turb_num in range(1, TURB_NUM + 1):
#         # yaw_wd_diff =  np.std(get_yaw_wd_diff(data, turb_num))
#         yaw_wd_diff =  get_yaw_wd_diff(data, turb_num)
#         file_dict[f'Turb_{turb_num}'] = yaw_wd_diff
#     df_yaw_wd = pd.DataFrame(file_dict)
#     df_yaw_wd.to_csv(f'./F01_yaw/wd_diff/yaw_wd_diff.csv_{file_name}', index = False)
# #     results.append(file_dict)
# # df_yaw_wd = pd.DataFrame(results)
# # df_yaw_wd.to_csv('./F01_yaw/yaw_wd_diff.csv', index = False)




# 4. 1号 2号机组风向一致性，风向变化标准差
def get_wind_direction_consistency(data, turb_num1, turb_num2):
    wd1 = data[f'{turb_num1}_wind_direction1'] 
    wd2 = data[f'{turb_num2}_wind_direction1']
    yaw1 = data[f'{turb_num1}_position']
    yaw2 = data[f'{turb_num2}_position']
    wd_diff = np.abs(wd1 + yaw1 - wd2 - yaw2) % 360
    shortest_diff = np.where(wd_diff > 180, 360 - wd_diff, wd_diff)
    return (shortest_diff)

# results = []
# for file_name in all_entries:
#     file_dict = {'file_name': file_name}
#     data = read_turb_data(config['data']['splited_data_new'], file_name)
#     for turb_num in range(1, TURB_NUM):
#         yaw_wd_diff =  get_wind_direction_consistency(data, turb_num, turb_num + 1)
#         yaw_wd_diff =  np.std(get_wind_direction_consistency(data, turb_num, turb_num + 1))
#         file_dict[f'F_{turb_num},F_{turb_num + 1}'] = yaw_wd_diff
#     df_yaw_wd = pd.DataFrame(file_dict)
#     df_yaw_wd.to_csv(f'./F01_yaw/wd_real/yaw_wd_consistency.csv_{file_name}', index = False)
# #     results.append(file_dict)
# # df_yaw_wd = pd.DataFrame(results)
# df_yaw_wd.to_csv('./F01_yaw/yaw_wd_consistency.csv', index = False)


# 5. 机组风向变化标准差
def get_wd_change(data, turb_num):
    wd = data[f'{turb_num}_wind_direction1'] 
    yaw = data[f'{turb_num}_position']
    wd_diff = np.abs(wd + yaw) % 360
    # shortest_diff = np.where(wd_diff > 180, 360 - wd_diff, wd_diff)
    return np.std(wd_diff)


# results = []
# for file_name in all_entries:
#     file_dict = {'file_name': file_name}
#     data = read_turb_data(config['data']['splited_data_new'], file_name)
#     for turb_num in range(1, TURB_NUM + 1):
#         wd_std =  get_wd_change(data, turb_num)
#         file_dict[f'F_{turb_num}'] = wd_std
#     results.append(file_dict)
# df_yaw_wd = pd.DataFrame(results)
# df_yaw_wd.to_csv('./F01_yaw/wd_change.csv', index = False)








