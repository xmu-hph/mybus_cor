import pandas as pd
import numpy as np
import json
import random
# 设置随机种子
random.seed(42)
from collections import defaultdict
from datetime import datetime, timedelta
import concurrent.futures
import time
from loguru import logger

class timetable():
    def __init__(self,card_data_path = './card_info.csv',\
                 bus_data_path = '公交数据.xlsx',\
                 road_line_station_setting_path = './datas/road_line_station_structure_setting.json',\
                 station_distance_rank_path = './datas/station_pair_distance_rank_index.csv',\
                 id_2_station_path = './datas/id_2_station.json',\
                 station_2_id_path = './datas/station_2_id.json'):
        self.card_data_path = card_data_path
        self.bus_data_path = bus_data_path
        self.road_line_station_setting_path = road_line_station_setting_path
        self.station_distance_rank_path = station_distance_rank_path
        self.id_2_station_path = id_2_station_path
        self.station_2_id_path = station_2_id_path
        process_start_time = time.time()
        self.process_data()
        logger.info(f"timetable process time:{time.time()-process_start_time}")
        
    def process_data(self):
        card_info_dataframe = pd.read_csv(self.card_data_path)
        bus_station_dataframe = pd.read_excel(self.bus_data_path,sheet_name='站点数据',header=0,engine='openpyxl')
        with open(self.road_line_station_setting_path, 'r') as f:
            road_line_station_structure_setting = json.load(f)
        self.road_line_station_structure_setting = road_line_station_structure_setting
        station_distance_rank_index_list = np.loadtxt(self.station_distance_rank_path,delimiter=',',dtype=int)
        self.station_distance_rank_index_list = station_distance_rank_index_list
        with open(self.id_2_station_path, 'r') as file:
            id_2_station = json.load(file)
            id_2_station = {int(key): value for key, value in id_2_station.items()}
        self.id_2_station = id_2_station
        with open(self.station_2_id_path, 'r') as file:
            station_2_id = json.load(file)
        self.station_2_id = station_2_id
        line_station_number_sequen_dataframe = \
                    bus_station_dataframe.groupby(['line_identity','station_identity']).agg({'station_name':list}).reset_index()
        line_station_number_sequen_dataframe['length'] = line_station_number_sequen_dataframe['station_name'].apply(lambda x:len(x))
        line_station_number_sequen_dataframe['station_name'] = line_station_number_sequen_dataframe['station_name'].apply(lambda x:x[0])
        line_station_number_sequen_dataframe = line_station_number_sequen_dataframe.\
            groupby('line_identity').agg({'station_identity':list,\
                                          'station_name':list}).reset_index()
        line_station_number_sequen_dataframe['station_name'] = line_station_number_sequen_dataframe.apply(lambda x:list(zip(x['station_identity'],x['station_name'])),axis=1)
        line_station_number_sequen_dataframe['station_name'] = line_station_number_sequen_dataframe['station_name'].apply(lambda x:sorted(x,key=lambda y:y[0]))
        road_section_2_lines = {}#格式：{shangdi_longze:[13,15]}路段到线路
        #逐线路的添加路段
        for row_index in range(len(line_station_number_sequen_dataframe)):
            line_station_number_sequen_dataframe_row = line_station_number_sequen_dataframe[row_index:row_index+1]
            line_identity:int = line_station_number_sequen_dataframe_row['line_identity'].values[0]
            stations_list:list = line_station_number_sequen_dataframe_row['station_name'].values[0]
            for start_order in range(len(stations_list)-1):
                start_station = stations_list[start_order]
                end_station = stations_list[start_order+1]
                start_station_order = station_2_id[start_station[1]]
                end_station_order = station_2_id[end_station[1]]
                road_section_2_lines_key = f"{start_station_order}_{end_station_order}"
                if road_section_2_lines_key in road_section_2_lines:
                    road_section_2_lines[road_section_2_lines_key].append(line_identity)
                else:
                    road_section_2_lines[road_section_2_lines_key]=[line_identity]
        self.road_section_2_lines = road_section_2_lines

        lines_list = line_station_number_sequen_dataframe['line_identity'].values
        self.lines_list = lines_list#格式：[1,2,3,13,15]线路列表

        card_info_dataframe['custom_day'] = card_info_dataframe['custom_time'].apply(lambda x:x.split(' ')[0])
        card_info_dataframe['custom_precise_time'] = card_info_dataframe['custom_time'].apply(lambda x:x.split(' ')[1])
        card_info_dataframe.drop(labels=['custom_time','card_type','consume','data_src','data_load_time','partitionday'],axis=1,inplace=True)
        day_0929_dataframe = card_info_dataframe[card_info_dataframe['custom_day']=='2023-09-29']
        def judge_whether_in(x,road_line_station_structure_setting):
            if str(x['line_identity']) in road_line_station_structure_setting and str(x['station_identity']) in road_line_station_structure_setting[str(x['line_identity'])]['stations']:
                return True
            else:
                return False
        day_0929_dataframe['judge'] = day_0929_dataframe.apply(lambda x:judge_whether_in(x,road_line_station_structure_setting),axis=1)
        day_0929_dataframe = day_0929_dataframe[day_0929_dataframe['judge']].reset_index(drop=True)
        day_0929_line_car_dataframe = day_0929_dataframe.groupby('line_identity').\
                    agg({'car_identity':set},axis=1).reset_index()
        day_0929_line_car_dataframe['car_identity'] = \
                    day_0929_line_car_dataframe.apply(lambda x:list(x['car_identity']),axis=1)
        line_2_buses = {}#格式：{13:[4527,4528]}线路到公交车，最终的思路是站点的时间表，注意这个变化
        for row_index in range(len(day_0929_line_car_dataframe)):
            row = day_0929_line_car_dataframe[row_index:row_index+1]
            line_identity = row['line_identity'].values[0]
            car_identity = row['car_identity'].values[0]
            line_2_buses[line_identity] = car_identity
        self.line_2_buses = line_2_buses

        day_0929_dataframe_in_lines_list = day_0929_dataframe[day_0929_dataframe['line_identity'].isin(lines_list)].reset_index(drop=True)
        every_user_all_trips_dataframe_0929 = \
                    day_0929_dataframe_in_lines_list.groupby('card_identity').agg({'custom_precise_time':list, \
                                                 'line_identity':list, \
                                                 'station_identity':list, \
                                                'car_identity':list},axis=1).reset_index()
        every_user_all_trips_dataframe_0929['trip_length'] = \
                    every_user_all_trips_dataframe_0929['line_identity'].apply(lambda x:len(x))
        trip_great_than_2_0929= \
                    every_user_all_trips_dataframe_0929[ \
                    every_user_all_trips_dataframe_0929['trip_length']!=1].reset_index(drop=True)
        trip_great_than_2_0929['trip'] = trip_great_than_2_0929.apply(lambda x:list(zip(x['custom_precise_time'],x['line_identity'],x['station_identity'],x['car_identity'])),axis=1)
        trip_great_than_2_0929['ordered_trips'] = \
                    trip_great_than_2_0929['trip'].apply(lambda x:sorted(x,key=lambda x:x[0]))
        station_2_lines_order = {}
        #格式：{station:{line1:[order],line2:[]}}
        #站点到线路的字典，以及在这个线路中的顺序
        for line_id in road_line_station_structure_setting.keys():
            line_stations = road_line_station_structure_setting[line_id]['stations']
            for station_id,station_name in line_stations.items():
                station_global_id = station_2_id[station_name]
                if station_global_id not in station_2_lines_order:
                    station_2_lines_order[station_global_id] = {}
                    station_2_lines_order[station_global_id][int(line_id)]=[int(station_id)]
                else:
                    if int(line_id) not in station_2_lines_order[station_global_id]:
                        station_2_lines_order[station_global_id][int(line_id)] = [int(station_id)]
                    else:
                        station_2_lines_order[station_global_id][int(line_id)].append(int(station_id))
        self.station_2_lines_order = station_2_lines_order

        trip_great_than_2_0929['od_trip_time_line']= \
                    trip_great_than_2_0929['ordered_trips'].apply(lambda x:self.get_od_pair(x))
        
        trip_great_than_2_0929_exploded = trip_great_than_2_0929.explode('od_trip_time_line').reset_index(drop=True)
        trip_great_than_2_0929_exploded['this_od_trip_start_station']= \
                    trip_great_than_2_0929_exploded['od_trip_time_line'].apply(lambda x:get_start_station(x))
        trip_great_than_2_0929_exploded['this_od_end_station']= \
                    trip_great_than_2_0929_exploded['od_trip_time_line'].apply(lambda x:get_leave_station(x))
        trip_great_than_2_0929_exploded['this_od_start_time']= \
                    trip_great_than_2_0929_exploded['od_trip_time_line'].apply(lambda x:get_this_time(x))
        trip_great_than_2_0929_exploded['this_od_by_line']= \
                    trip_great_than_2_0929_exploded['od_trip_time_line'].apply(lambda x:get_this_line(x))
        trip_great_than_2_0929_exploded['this_od_by_car']= \
                    trip_great_than_2_0929_exploded['od_trip_time_line'].apply(lambda x:get_this_car(x))
        trip_great_than_2_0929_exploded['forward'] = \
                    trip_great_than_2_0929_exploded.\
                    apply(lambda x:x['this_od_trip_start_station']<x['this_od_end_station'],axis=1)
        trip_great_than_2_0929_exploded = trip_great_than_2_0929_exploded[trip_great_than_2_0929_exploded['forward']].reset_index(drop=True)

        transfer = {'this_od_trip_start_station':'station_identity',\
            'this_od_by_car':'car_identity',\
            'this_od_start_time':'custom_precise_time',\
            'this_od_by_line':'line_identity',\
            'card_identity':'card_identity'}
        trip_1_0929 = \
                    every_user_all_trips_dataframe_0929\
                    [every_user_all_trips_dataframe_0929\
                    ['trip_length'] ==1].reset_index(drop=True)
        trip_1_0929['trip'] = trip_1_0929.\
                    apply(lambda x:list(zip(x['custom_precise_time'],\
                            x['line_identity'],x['station_identity'],x['car_identity'])),axis=1)
        trip_1_0929_exploded = trip_1_0929.explode('trip').reset_index(drop=True)
        trip_1_0929_exploded['this_od_trip_start_station']= \
                    trip_1_0929_exploded['trip'].apply(lambda x:x[2])
        trip_1_0929_exploded['this_od_start_time']= \
                    trip_1_0929_exploded['trip'].apply(lambda x:x[0])
        trip_1_0929_exploded['this_od_by_line']= \
                    trip_1_0929_exploded['trip'].apply(lambda x:x[1])
        trip_1_0929_exploded['this_od_by_car']= \
                    trip_1_0929_exploded['trip'].apply(lambda x:x[3])
        need_attrs = ['card_identity','this_od_by_line','this_od_by_car','this_od_trip_start_station','this_od_start_time']
        #match_attrs = ['card_identity','line_identity','car_identity','station_identity','custom_precise_time']
        
        temp1 = trip_great_than_2_0929_exploded[\
                    need_attrs]
        temp2 = trip_1_0929_exploded[need_attrs]
        temp1.rename(columns = transfer,inplace=True)
        temp2.rename(columns = transfer,inplace=True)
        forward_trip = pd.concat([temp1,temp2[temp1.columns]],axis=0).reset_index(drop=True)
        forward_trip = forward_trip[forward_trip['line_identity'].isin(lines_list)].reset_index(drop=True)
        day_0929_bus_info_dataframe = \
                    forward_trip.groupby('car_identity').agg({'custom_precise_time':list, \
                                                 'line_identity':set, \
                                                 'station_identity':list, \
                                                'card_identity':list},axis=1).reset_index()
        day_0929_bus_info_dataframe['time_station']=day_0929_bus_info_dataframe.apply(lambda x:list(zip(x['custom_precise_time'],x['station_identity'])),axis=1)
        day_0929_bus_info_dataframe['sort_time_station']= day_0929_bus_info_dataframe.apply(lambda x:\
                               sorted(x['time_station'],key=lambda y:y[0])\
                               ,axis=1)
        day_0929_bus_info_dataframe['sort_time_station'] = day_0929_bus_info_dataframe.apply(lambda x:split_into_groups(x['sort_time_station']),axis=1)
        day_0929_bus_info_dataframe['remove_dupilicate']=\
                    day_0929_bus_info_dataframe.\
                    apply(lambda x:\
                    [split_into_time_dict(data) for \
                    data in x['sort_time_station']],axis=1)
        
        arrive_time_2_station_id = {}
        for row_index in range(len(day_0929_bus_info_dataframe)):
            #每一行都是一辆车的信息
            row = day_0929_bus_info_dataframe[row_index:row_index+1]
            car_identity = row['car_identity'].values[0]#车牌号
            line_identity = list(row['line_identity'].values[0])[0]#线路名
            remove_dupilicate = row['remove_dupilicate'].values[0]#运行时间列表
            #每条线路有哪些站点？
            line_info = road_line_station_structure_setting[str(line_identity)]
            #line_info_stations_nums = line_info['stations_nums']
            line_info_stations_dict = line_info['stations']#线路站点编号到名字的映射
            #这个公交到达各个站点的时间？
            #arrive_time_2_station_id = {}
            #row_station_id_2_arrive_time = {}
            for row_id in range(len(remove_dupilicate)):
                row = remove_dupilicate[row_id]#每一个运行时间表
                columns_list = list(line_info_stations_dict.keys())
                columns_list = sorted(columns_list,key=lambda x:int(x))
                #columns_list.sort(key=lambda x:int(x))
                temp_arrive_time_2_station_id = dict(zip(\
                    columns_list,\
                        [None]*len(columns_list)))
                for data_instance in row:
                    time = data_instance[0]
                    station_id = data_instance[1]
                    #station_name = line_info_stations_dict[str(station_id)]
                    #station_name_2_id = station_2_id[station_name]
                    temp_arrive_time_2_station_id[str(station_id)]=time
                if car_identity not in arrive_time_2_station_id:
                    arrive_time_2_station_id[car_identity]=[temp_arrive_time_2_station_id]
                else:
                    arrive_time_2_station_id[car_identity].append(temp_arrive_time_2_station_id)
        self.arrive_time_2_station_id = arrive_time_2_station_id
        

        # 并行处理字典中的每个 DataFrame
        process_time_dict = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_dataframe, key, arrive_time_2_station_id[key]): key for key in arrive_time_2_station_id.keys()}
            for future in concurrent.futures.as_completed(futures):
                key, result_df,process_time = future.result()
                arrive_time_2_station_id[key] = result_df
                process_time_dict[key] = process_time
        executor.shutdown(wait=False)
        self.process_time_dict = process_time_dict

        line_2_time_df = {}
        for line_id in line_2_buses.keys():
            buses_list = line_2_buses[line_id]
            total_df = None
            for bus_id in buses_list:
                if bus_id in arrive_time_2_station_id:
                    total_df = pd.concat([total_df, arrive_time_2_station_id[bus_id]], ignore_index=True) if total_df is not None else arrive_time_2_station_id[bus_id]
            line_2_time_df[line_id] = total_df
        
        new_temp = {}
        for key in line_2_time_df.keys():
            if line_2_time_df[key] is None:
                new_temp[key] = line_2_time_df[key]
                continue
            new_temp[key] = line_2_time_df[key].apply(lambda x:check_time_relationship(x), axis=1)
            new_temp[key] = fill_weighted_average_updown(new_temp[key]) \
                        if new_temp[key] is not None else new_temp[key]
            new_temp[key] = new_temp[key].apply(fill_weighted_average, axis=1)
            new_temp[key] = new_temp[key].apply(fill_weighted_average_left, axis=1)
            new_temp[key] = new_temp[key].apply(fill_weighted_average_right, axis=1)
            new_temp[key] = \
                        new_temp[key].sort_values(\
                        by=list(new_temp[key].columns)).reset_index(drop=True) if new_temp[key] \
                        is not None else new_temp[key]
        self.time_table = new_temp

    def find_leave_station(self,sorted_index_array:np.ndarray,specified_indices:list):
        # 找到指定索引组中在排序数组中的位置
        positions = [sorted_index_array.index(idx) for idx in specified_indices]
        return self.station_2_lines_order[sorted_index_array[min(positions)]]
    def find_closest(self,lst, target):
        closest_value = min(lst, key=lambda x: abs(x - target))
        return closest_value
    def get_od_pair(self,x):
        od_trips=[]
        for i in range(len(x)-1):
            this_time,this_line,this_station_id,car_1 = x[i]
            next_time,next_line,next_station_id,car_2 = x[i+1]
            try:
                #print(this_time,this_line,this_station_id)
                #print(next_time,next_line,next_station_id)
                this_line_station_name = self.road_line_station_structure_setting[str(this_line)]['stations'][str(this_station_id)]
                this_line_station_id = self.station_2_id[this_line_station_name]
                #print(i,this_line_station_name,this_line_station_id)
                all_possible_leave_stations = self.road_line_station_structure_setting[str(this_line)]['all_stations']
                all_possible_leave_stations_index = [self.station_2_id[station] for station in all_possible_leave_stations]
                #print(all_possible_leave_stations_index)
                next_line_station_name = self.road_line_station_structure_setting[str(next_line)]['stations'][str(next_station_id)]
                next_line_station_id = self.station_2_id[next_line_station_name]
                #print(next_line_station_id)
                distance_2_next_line_station_ndarray  = self.station_distance_rank_index_list[next_line_station_id]
                #leave_station = find_leave_station(distance_2_next_line_station_ndarray.tolist(),all_possible_leave_stations_index)
                leave_station_dict = self.find_leave_station(distance_2_next_line_station_ndarray.tolist(),all_possible_leave_stations_index)
                leave_station = self.find_closest(leave_station_dict[this_line], this_line_station_id)
                #print(this_line_station_id,leave_station,this_time,this_line)
                od_trip = {'this_line_station_id':this_station_id, \
                        'leave_station':leave_station,\
                        'this_time':this_time,'this_line':this_line,'this_car':car_1}
                #print(od_trip)
                od_trips.append(od_trip)
            except:
                True
        return od_trips
def get_start_station(x):
    if not isinstance(x,float):
        return x['this_line_station_id']
    else:
        return -1
def get_leave_station(x):
    if not isinstance(x,float):
        return x['leave_station']
    else:
        return -1
def get_this_time(x):
    if not isinstance(x,float):
        return x['this_time']
    else:
        return -1
def get_this_line(x):
    if not isinstance(x,float):
        return x['this_line']
    else:
        return -1
def get_this_car(x):
    if not isinstance(x,float):
        return x['this_car']
    else:
        return -1
def split_into_groups(data):
    groups = []
    start = 0
    for i in range(1, len(data)):
        if data[i][1] < data[i-1][1]:  # 如果当前元素小于等于前一个元素，说明递增序列断开
            groups.append(data[start:i])
            start = i
    # 添加最后一个分组
    groups.append(data[start:])
    return groups
def split_into_time_dict(data):
    min_times = defaultdict(lambda: '24:24:24')  # 初始化为正无穷大
    # 遍历数据并更新每个站点的最小时间
    for time, station in data:
        if time < min_times[station]:
            min_times[station] = time
    # 将字典中的值转换为列表
    result1 = [(time, station) for station, time in min_times.items()]
    # 根据站点编号排序
    result1.sort(key=lambda x: x[1])
    return result1
def parse_time_columns(df):
    for col in df.columns:
        df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time
    return df
def time_to_seconds(t):
    """将时间对象转换为秒数"""
    return t.hour * 3600 + t.minute * 60 + t.second
def seconds_to_time(s):
    """将秒数转换为时间对象"""
    return (datetime.min + timedelta(seconds=s)).time()
def fill_weighted_average_updown(df):
    # 对每个空缺值进行处理
    filled_df = df
    total_column = np.array(df.notnull())
    for col in range(len(df.columns)):
        for i in range(len(df)):
            non_null_indices = total_column[:, col]#total_column.iloc[:, col]#df.iloc[:, col].notnull()
            column1 = total_column[i,:]#total_column.iloc[i,:]#df.iloc[i,:].notnull()
            #pd.isnull(df.iloc[i, col])
            if not column1[col]:
                #column1.iloc[col]:
                try:
                    result_indices = ((column1 & total_column).sum(axis=1) >0) & non_null_indices
                    if result_indices.sum() == 0:
                        continue
                    prev_idx = result_indices.argmax()#row
                    column_next_idx = (column1 & total_column[prev_idx,:]).argmax()#column
                    prev_prev_time = time_to_seconds(df.iloc[prev_idx, column_next_idx])
                    prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                    this_next_time = time_to_seconds(df.iloc[i,column_next_idx])
                    filled_time = this_next_time + (prev_next_time - prev_prev_time)
                    # 检查时间是否前后合理
                    if col!=0 and col-1-column1[:col][::-1].argmax()>-1:
                        before_time = \
                            time_to_seconds(df.iloc[i, col-1-column1[:col][::-1].argmax()])
                        if filled_time > before_time:
                            filled_df.iloc[i, col]  = seconds_to_time(filled_time)
                            total_column[i, col] = True
                        else:
                            continue
                    else:
                        filled_df.iloc[i, col]  = seconds_to_time(filled_time)
                        total_column[i, col] = True
                except:
                    continue
    return filled_df
def process_dataframe(key, df):
    start_time = time.time()
    df = pd.DataFrame(df)
    temp = parse_time_columns(df)
    temp = fill_weighted_average_updown(temp)
    #temp = temp.apply(fill_weighted_average, axis=1)
    #temp = temp.apply(fill_weighted_average_left, axis=1)
    #temp = temp.apply(fill_weighted_average_right, axis=1)
    end_time = time.time()
    return key, temp, end_time-start_time
# 应用填充函数到DataFrame的每一行
# 中间填充
def fill_weighted_average(row):
    non_null_indices = row.notnull()
    if non_null_indices.sum() < 2:
        return row
    filled_row = row
    #filled_row = row.copy()
    for i in range(len(row)):
        if pd.isnull(row.iloc[i]):
            try:
                # 找到前后的最近非空值
                prev_idx = non_null_indices[:i][::-1].idxmax()
                next_idx = non_null_indices[i:].idxmax()
            
                if pd.isnull(prev_idx) or pd.isnull(next_idx):
                    continue

                prev_time = row[prev_idx]
                next_time = row[next_idx]
            
                # 计算距离
                distance_prev = int(row.keys()[i]) - int(prev_idx)
                distance_next = int(next_idx) - int(row.keys()[i])
                prev_time = time_to_seconds(prev_time)
                next_time = time_to_seconds(next_time)
                # 计算加权平均
                filled_time = prev_time + (next_time - prev_time) * (distance_prev / (distance_prev + distance_next))
            
                filled_row.iloc[i] = seconds_to_time(filled_time)
            except:
                continue
    return filled_row
# 应用填充函数到DataFrame的每一行
#向左填充
def fill_weighted_average_left(row):
    non_null_indices = row.notnull()
    if non_null_indices.sum() < 2:
        return row
    filled_row = row
    #filled_row = row.copy()
    for i in range(len(row)):
        if pd.isnull(row.iloc[i]):
            try:
                # 找到后方的最近两个非空值
                prev_idx = non_null_indices[i+1:].idxmax()
                next_idx = non_null_indices[list(row.keys()).index(prev_idx)+1:].idxmax()
                #row.keys().index(prev_idx)
                #prev_idx = non_null_indices[:i][::-1].idxmax()
                #next_idx = non_null_indices[i:].idxmax()
            
                if pd.isnull(prev_idx) or pd.isnull(next_idx):
                    continue

                prev_time = row[prev_idx]
                next_time = row[next_idx]
            
                # 计算距离
                distance_prev = int(row.keys()[i]) - int(prev_idx)
                distance_next = int(next_idx) - int(prev_idx)
                prev_time = time_to_seconds(prev_time)
                next_time = time_to_seconds(next_time)
                # 计算加权平均
                filled_time = prev_time + (next_time - prev_time) * (distance_prev / distance_next)
            
                filled_row.iloc[i] = seconds_to_time(filled_time)
            except:
                continue
    return filled_row
# 应用填充函数到DataFrame的每一行
#向右填充
def fill_weighted_average_right(row):
    non_null_indices = row.notnull()
    if non_null_indices.sum() < 2:
        return row
    filled_row = row
    #filled_row = row.copy()
    for i in range(len(row)):
        if pd.isnull(row.iloc[i]):
            try:
                # 找到前方的最近两个非空值
                next_idx = non_null_indices[:i][::-1].idxmax()
                prev_idx = non_null_indices[:list(row.keys()).index(next_idx)][::-1].idxmax()
            
                if pd.isnull(prev_idx) or pd.isnull(next_idx):
                    continue

                prev_time = row[prev_idx]
                next_time = row[next_idx]
            
                # 计算距离
                distance_prev = int(row.keys()[i]) - int(prev_idx)
                distance_next = int(next_idx) - int(prev_idx)
                prev_time = time_to_seconds(prev_time)
                next_time = time_to_seconds(next_time)
                # 计算加权平均
                filled_time = prev_time + (next_time - prev_time) * (distance_prev / distance_next)
            
                filled_row.iloc[i] = seconds_to_time(filled_time)
            except:
                continue
    return filled_row
# 应用填充函数到DataFrame
def check_time_relationship(row):
    # 遍历每一行的时间列
    for col in range(len(row)):
        current_time = row.iloc[col]
        if pd.isnull(current_time):
            continue
        prev_col = None
        # 找到当前列的前一列
        for c in range(col - 1, -1, -1):
            if pd.notnull(row.iloc[c]):
                prev_col = row.iloc[c]
                break
        if prev_col and (not (time_to_seconds(prev_col) <= time_to_seconds(current_time))):
            row.iloc[col] = pd.NaT
    return row
