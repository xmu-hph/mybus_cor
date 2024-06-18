from timetable import timetable
from passenger import simu
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--card_data_path',default='./card_info.csv')
parser.add_argument('--bus_data_path',default= './公交数据.xlsx')
parser.add_argument('--road_line_station_setting_path',default= './datas/road_line_station_structure_setting.json')
parser.add_argument('--station_distance_rank_path',default= './datas/station_pair_distance_rank_index.csv')
parser.add_argument('--id_2_station_path',default= './datas/id_2_station.json')
parser.add_argument('--station_2_id_path',default= './datas/station_2_id.json')
myargs = parser.parse_args()
mytime_table = timetable(\
    card_data_path = myargs.card_data_path,\
    bus_data_path = myargs.bus_data_path,\
    road_line_station_setting_path = myargs.road_line_station_setting_path,\
    station_distance_rank_path = myargs.station_distance_rank_path,\
    id_2_station_path = myargs.id_2_station_path,\
    station_2_id_path = myargs.station_2_id_path)
this_simu = simu(\
    mytime_table,\
    card_data_path = myargs.card_data_path,\
    bus_data_path = myargs.bus_data_path,\
    road_line_station_setting_path = myargs.road_line_station_setting_path,\
    station_distance_rank_path = myargs.station_distance_rank_path,\
    id_2_station_path = myargs.id_2_station_path,\
    station_2_id_path = myargs.station_2_id_path,\
    time_accurate = 8,\
    start_hour = 8,\
    start_minute = 23)
this_simu.step()
this_simu.observe()