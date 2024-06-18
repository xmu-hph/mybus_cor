now_time = str(this_simu.now_hour)+':'+str(this_simu.now_minute)+':00'
now_time = pd.to_datetime(now_time, format='%H:%M:%S', errors='coerce').dt.time
road_section_people_nums = defaultdict(int)
for user in this_simu.total_passengers:
    if user['time_table'].iloc[-1]<now_time:
        continue
    else:
        column_index = user['time_table'][user['time_table']<=now_time].index.max()
        #根据当前线路和站点编号来确定路段。
        this_line = user['target_line']
        start_station = this_simu.road_line_station_structure_setting['this_line']['stations'][column_index]
        end_station = this_simu.road_line_station_structure_setting['this_line']['stations'][column_index+1]
        road_section = f"{this_simu.station_2_id[start_station]}_{this_simu.station_2_id[end_station]}"
        road_section_people_nums[road_section] +=1
this_simu.road_section_people_nums = road_section_people_nums