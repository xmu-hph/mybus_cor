this_simu.now_minute += 1
if this_simu.now_minute == 60:
    this_simu.now_minute = 0
    this_simu.now_hour += 1
if this_simu.now_hour == 24:
    this_simu.now_hour = 0
station_id_list = list(this_simu.id_2_station.keys())
now_conditions_df=pd.DataFrame({'this_od_trip_start_station':station_id_list,\
                                        'hour':[this_simu.now_hour]*len(station_id_list),\
                                        'minute_group':[this_simu.now_minute//this_simu.time_accurate]*len(station_id_list)})
now_user_all_possible_route_df = now_conditions_df.merge(\
            this_simu.different_time_user_produce, \
            left_on=['this_od_trip_start_station', 'hour', 'minute_group'],\
            right_on = ['global_station_id','hour','minute_group'], how='left').fillna(0)
        #now_user_all_possible_route_df['zip_target_station_line'] = now_user_all_possible_route_df.apply(lambda x:zip_target_station_line_func(x),axis=1)
now_station_routes_and_nums = \
                    now_user_all_possible_route_df.merge(this_simu.different_time_user_produce_nums,\
                                     left_on=['this_od_trip_start_station', 'hour', 'minute_group'],\
                                     right_on = ['this_od_trip_start_station', 'hour', 'minute_group'],\
                                     how='left').fillna(0)
now_station_routes_and_nums['users'] = now_station_routes_and_nums.apply(lambda x:uniform_sample(x),axis=1)
now_passengers = []
for row_index in range(len(now_station_routes_and_nums)):
    if len(now_station_routes_and_nums[row_index:row_index+1]['users'].values[0])!=0:
        #print(merged_df[row_index:row_index+1]['users'].values[0])
        start_station = now_station_routes_and_nums[row_index:row_index+1]['this_od_trip_start_station'].values[0]
        start_time = str(this_simu.now_hour)+':'+str(this_simu.now_minute)+':00'
        start_time = pd.to_datetime(start_time, format='%H:%M:%S', errors='coerce').dt.time
        #start_time = now_station_routes_and_nums[row_index:row_index+1]['minute_group'].values[0]
        for user_instance in now_station_routes_and_nums[row_index:row_index+1]['users'].values[0]:
            now_passengers.append({\
                        'start_station':user_instance['this_od_trip_start_station'],\
                        'start_time':start_time,\
                        'target_station':user_instance['this_od_end_station'],\
                        'target_line':user_instance['this_od_by_line']})
        #根据时间表为每个乘客添加时间表
for user in now_passengers:
    if user['start_station'] < user['target_station'] and this_simu.time_schedule.time_table[user['target_line']]:
        time_df = this_simu.time_schedule.time_table[user['target_line']]
        time_df_column = time_df.loc[:,user['start_station']]
        row_index = time_df_column[time_df_column>=user['start_time']].index.max()
        column_index = time_df.columns.get_loc(user['start_station'])
        user['time_table'] = time_df.iloc[row_index,column_index:]
        this_simu.total_passengers.append(user)
    else:
        continue