import pandas as pd
import numpy as np
import json
import csv
import random
# 设置随机种子
random.seed(42)
from timetable import timetable
from collections import defaultdict
from loguru import logger
import time


class simu():
    def __init__(self,time_schedule,card_data_path = './card_info.csv',\
                 bus_data_path = '公交数据.xlsx',\
                 road_line_station_setting_path = './datas/road_line_station_structure_setting.json',\
                 station_distance_rank_path = './datas/station_pair_distance_rank_index.csv',\
                 id_2_station_path = './datas/id_2_station.json',\
                 station_2_id_path = './datas/station_2_id.json',\
                 time_accurate = 8,\
                 start_hour = 8,\
                 start_minute = 23):
        self.card_data_path = card_data_path
        self.bus_data_path = bus_data_path
        self.road_line_station_setting_path = road_line_station_setting_path
        self.station_distance_rank_path = station_distance_rank_path
        self.id_2_station_path = id_2_station_path
        self.station_2_id_path = station_2_id_path
        self.time_accurate = time_accurate
        self.now_hour = start_hour
        self.now_minute = start_minute
        self.total_passengers = []
        self.time_schedule = time_schedule
        self.road_section_people_nums = defaultdict(int)
        process_start_time = time.time()
        self.process_data()
        logger.info(f"process time:{time.time()-process_start_time}")

    def process_data(self):
        card_info_dataframe = pd.read_csv(self.card_data_path)
        bus_station_dataframe = pd.read_excel(self.bus_data_path,sheet_name='站点数据',header=0,engine='openpyxl')
        card_info_dataframe['custom_day'] = card_info_dataframe['custom_time'].apply(lambda x:x.split(' ')[0])
        card_info_dataframe['custom_precise_time'] = card_info_dataframe['custom_time'].apply(lambda x:x.split(' ')[1])
        card_info_dataframe.drop(labels=['custom_time','card_type','consume','data_src','data_load_time','partitionday'],axis=1,inplace=True)
        day_0929_dataframe = card_info_dataframe[card_info_dataframe['custom_day']=='2023-09-29']
        every_user_all_trips_dataframe_0929 = \
                    day_0929_dataframe.groupby('card_identity').agg({'custom_precise_time':list, \
                                                'line_identity':list, \
                                                'station_identity':list, \
                                                'car_identity':list},axis=1).reset_index()
        every_user_all_trips_dataframe_0929['trip_length'] = \
                    every_user_all_trips_dataframe_0929['line_identity'].apply(lambda x:len(x))
        trip_great_than_2_0929= \
                    every_user_all_trips_dataframe_0929[ \
                    every_user_all_trips_dataframe_0929['trip_length']!=1].reset_index(drop=True)
        trip_great_than_2_0929['trip'] = trip_great_than_2_0929.apply(lambda x:list(zip(x['custom_precise_time'],x['line_identity'],x['station_identity'])),axis=1)
        trip_great_than_2_0929['ordered_trips'] = \
                    trip_great_than_2_0929['trip'].apply(lambda x:sorted(x,key=lambda x:x[0]))
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
        
        station_2_lines_order = {}#{station:{line1:[order],line2:[]}}
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
        trip_great_than_2_0929_exploded['forward'] = \
                    trip_great_than_2_0929_exploded.\
                    apply(lambda x:x['this_od_trip_start_station']<=x['this_od_end_station'],axis=1)
        trip_great_than_2_0929_exploded = trip_great_than_2_0929_exploded[trip_great_than_2_0929_exploded['forward']].reset_index(drop=True)
        trip_great_than_2_0929_exploded['global_station_id'] = trip_great_than_2_0929_exploded.apply(lambda x:self.get_station_id(x['this_od_by_line'],x['this_od_trip_start_station']),axis=1)
        
        need_attributes = ['this_od_trip_start_station','this_od_end_station','this_od_start_time','this_od_by_line','global_station_id']
        all_known_od_trips_0929 = trip_great_than_2_0929_exploded[need_attributes]
        can_use_all_known_od_trips_0929 = all_known_od_trips_0929[all_known_od_trips_0929['this_od_trip_start_station']!=-1].reset_index(drop=True)
        can_use_all_known_od_trips_0929['hour'] = can_use_all_known_od_trips_0929['this_od_start_time'].apply(lambda x:int(x.split(':')[0]))
        can_use_all_known_od_trips_0929['min'] = can_use_all_known_od_trips_0929['this_od_start_time'].apply(lambda x:int(x.split(':')[1]))
        can_use_all_known_od_trips_0929['second'] = can_use_all_known_od_trips_0929['this_od_start_time'].apply(lambda x:int(x.split(':')[2]))
        time_split_array = list(range(0,60+self.time_accurate,self.time_accurate))
        labels = list(range(len(time_split_array)-1))
        can_use_all_known_od_trips_0929['minute_group'] = pd.cut(can_use_all_known_od_trips_0929['min'], bins=time_split_array, right=False, labels=labels)
        can_use_all_known_od_trips_0929['od_trip'] = \
            can_use_all_known_od_trips_0929.apply(\
                lambda x:{'start_station':x['this_od_trip_start_station'],\
                          'end_station':x['this_od_end_station'],\
                          'this_line':x['this_od_by_line']},axis=1)
        different_time_user_produce = \
                    can_use_all_known_od_trips_0929.\
                    groupby(['global_station_id','hour','minute_group'],observed=False)\
                    .agg({'od_trip':list}).reset_index()
        def get_len(x):
            if isinstance(x,float):
                return 0
            else:
                return len(x)
        different_time_user_produce['length'] = different_time_user_produce.apply(lambda x:get_len(x['od_trip']),axis=1)
        self.different_time_user_produce = different_time_user_produce

        day_0929_dataframe['this_od_trip_start_station'] = day_0929_dataframe.apply(lambda x: self.get_station_id(x['line_identity'],x['station_identity']),axis=1)
        can_use_user_data = day_0929_dataframe[day_0929_dataframe['this_od_trip_start_station']!=-1].reset_index(drop=True)[['custom_precise_time','this_od_trip_start_station']]
        can_use_user_data['hour'] = can_use_user_data.apply(lambda x:int(x['custom_precise_time'].split(':')[0]),axis=1)
        can_use_user_data['minute'] = can_use_user_data.apply(lambda x:int(x['custom_precise_time'].split(':')[1]),axis=1)
        can_use_user_data['seconds'] = can_use_user_data.apply(lambda x:int(x['custom_precise_time'].split(':')[2]),axis=1)

        can_use_user_data['minute_group'] = pd.cut(can_use_user_data['minute'], bins=time_split_array, right=False, labels=labels)
        different_time_user_produce_nums = can_use_user_data.groupby(['this_od_trip_start_station','hour','minute_group'],observed=False).size().reset_index(name='count')
        self.different_time_user_produce_nums = different_time_user_produce_nums

    def step(self):
        self.now_minute += 1
        if self.now_minute == 60:
            self.now_minute = 0
            self.now_hour += 1
        if self.now_hour == 24:
            self.now_hour = 0
        station_id_list = list(self.id_2_station.keys())
        now_conditions_df=pd.DataFrame({'this_od_trip_start_station':station_id_list,\
                                        'hour':[self.now_hour]*len(station_id_list),\
                                        'minute_group':[self.now_minute//self.time_accurate]*len(station_id_list)})
        now_user_all_possible_route_df = now_conditions_df.merge(\
            self.different_time_user_produce, \
            left_on=['this_od_trip_start_station', 'hour', 'minute_group'],\
            right_on = ['global_station_id','hour','minute_group'], how='left').fillna(0)
        #now_user_all_possible_route_df['zip_target_station_line'] = now_user_all_possible_route_df.apply(lambda x:zip_target_station_line_func(x),axis=1)
        now_station_routes_and_nums = \
                    now_user_all_possible_route_df.merge(self.different_time_user_produce_nums,\
                                     left_on=['this_od_trip_start_station', 'hour', 'minute_group'],\
                                     right_on = ['this_od_trip_start_station', 'hour', 'minute_group'],\
                                     how='left').fillna(0)
        now_station_routes_and_nums['users'] = now_station_routes_and_nums.apply(lambda x:uniform_sample(x,self.time_accurate),axis=1)
        now_passengers = []
        for row_index in range(len(now_station_routes_and_nums)):
            if len(now_station_routes_and_nums[row_index:row_index+1]['users'].values[0])!=0:
                #print(merged_df[row_index:row_index+1]['users'].values[0])
                start_station = now_station_routes_and_nums[row_index:row_index+1]['this_od_trip_start_station'].values[0]
                start_time = str(self.now_hour)+':'+str(self.now_minute)+':00'
                start_time = pd.to_datetime(start_time, format='%H:%M:%S', errors='coerce').dt.time
                #start_time = now_station_routes_and_nums[row_index:row_index+1]['minute_group'].values[0]
                for user_instance in now_station_routes_and_nums[row_index:row_index+1]['users'].values[0]:
                    now_passengers.append({\
                        'start_station':user_instance['start_station'],\
                        'start_time':start_time,\
                        'target_station':user_instance['end_station'],\
                        'target_line':user_instance['this_line']})
        #根据时间表为每个乘客添加时间表
        for user in now_passengers:
            if user['start_station'] < user['target_station'] and self.time_schedule.time_table[user['target_line']] is not None:
                time_df = self.time_schedule.time_table[user['target_line']]
                time_df_column = time_df.loc[:,str(user['start_station'])]
                row_index = time_df_column[time_df_column>=user['start_time']].index.min()
                column_index = time_df.columns.get_loc(str(user['start_station']))
                user['time_table'] = time_df.iloc[row_index,column_index:]
                self.total_passengers.append(user)
            else:
                continue
    def observe(self):
        now_time = str(self.now_hour)+':'+str(self.now_minute)+':00'
        now_time = pd.to_datetime(now_time, format='%H:%M:%S', errors='coerce').dt.time
        road_section_people_nums = defaultdict(int)
        for user in self.total_passengers:
            if user['time_table'].iloc[-1]<now_time:
                continue
            else:
                column_index = user['time_table'][user['time_table']<=now_time].index.max()
                #根据当前线路和站点编号来确定路段。
                this_line = user['target_line']
                start_station = self.road_line_station_structure_setting['this_line']['stations'][column_index]
                end_station = self.road_line_station_structure_setting['this_line']['stations'][column_index+1]
                road_section = f"{self.station_2_id[start_station]}_{self.station_2_id[end_station]}"
                road_section_people_nums[road_section] +=1
        self.road_section_people_nums = road_section_people_nums

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
            this_time,this_line,this_station_id = x[i]
            next_time,next_line,next_station_id = x[i+1]
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
                            'this_time':this_time,'this_line':this_line}
                #print(od_trip)
                od_trips.append(od_trip)
            except:
                True
        return od_trips
    
    def get_station_id(self,line_id,order_id):
        try:
            return self.station_2_id[self.road_line_station_structure_setting[str(line_id)]['stations'][str(order_id)]]
        except:
            return -1
    
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
def zip_target_station_line_func(x):
    if isinstance(x['this_od_end_station'],list):
        return list(zip(x['this_od_end_station'],x['this_od_by_line']))
    else:
        return []
def uniform_sample(x,time_accurate):
    if x['length']!=0:
        #平均流量
        average_nums = int(x['count'])/time_accurate
        actual_nums = np.random.poisson(average_nums)
        return random.choices(x['od_trip'],k=actual_nums)
    else:
        return []