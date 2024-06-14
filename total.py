import pandas as pd
import numpy as np
import json
import csv
#1.确定这几个数据都是干什么的
bus_station_dataframe = pd.read_excel('公交数据.xlsx',sheet_name='站点数据',header=0,engine='openpyxl')
#用去年的时间的同比数据，去预测今年的同比数据，看是否一致，如果一致，说明经济发展没有变化，如果变大，说明经济好转
#bus_station_dataframe ： 站点的统计信息：各个站点的经纬度
line_station_number_sequen_dataframe = \
bus_station_dataframe.groupby(['line_identity','station_number']).agg({'station_name':list,'station_identity':list}).reset_index()
line_station_number_sequen_dataframe['length'] = line_station_number_sequen_dataframe['station_identity'].apply(lambda x:len(x))

line_station_number_sequen_dataframe['id_station'] = \
line_station_number_sequen_dataframe.apply(lambda x:list(zip(x['station_identity'],x['station_name'])),axis=1)

#构建一个字典：
#road_structure = {line_id:{'stations_nums':int,'stations':{station_id:station_name},'all_stations':[list]}}
road_line_station_structure_setting={}
for row_index in range(len(line_station_number_sequen_dataframe)):
    line_id = line_station_number_sequen_dataframe[row_index:row_index+1]['line_identity'].values[0]
    stations_nums = line_station_number_sequen_dataframe[row_index:row_index+1]['station_number'].values[0]
    all_stations = line_station_number_sequen_dataframe[row_index:row_index+1]['station_name'].values[0]
    stations={}
    for instance in line_station_number_sequen_dataframe[row_index:row_index+1]['id_station'].values[0]:
        instance_id = instance[0]
        instance_name = instance[1]
        stations[instance_id]=instance_name
    road_line = {'stations_nums':stations_nums,'stations':stations,'all_stations':all_stations}
    road_line_station_structure_setting[line_id] = road_line    
road_line_station_structure_setting