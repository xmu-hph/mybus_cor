import pandas as pd
import numpy as np
import json
station_pair_distance_ndarray = np.loadtxt('./station_pair_distance.csv',delimiter=',')
# 找出每行最小的k个值所对应的索引
station_distance_rank_index_list = [sorted(range(len(row)), key=lambda i: row[i]) for row in station_pair_distance_ndarray]
import csv
# 保存列表到文件中
with open('./station_pair_distance_rank_index.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(station_distance_rank_index_list)
print("文件保存成功！")