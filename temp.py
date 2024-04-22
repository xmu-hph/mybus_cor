#line_direct_seq_stat_nums
#需要整理为 station：可达station，线路，站点编号
station_line_arrive_sequen={}
for line in line_direct_seq_stat_nums.keys():
    direction_list = list(line_direct_seq_stat_nums[line].keys())
    direction_list.sort()
    if len(direction_list)==1:
        direction=direction_list[0]
        sequen = list(line_direct_seq_stat_nums[line][direction].keys())
        sequen.sort()
        for index in range(len(sequen)):
            start_station = line_direct_seq_stat_nums[line][direction][sequen[index]][0]
            if index==0:
                temp_range=0
            else:
                temp_range=1
            for target_index in range(index+1,len(sequen)+temp_range):
                target_station = line_direct_seq_stat_nums[line][direction][sequen[target_index%len(sequen)]][0]
                if start_station not in station_line_arrive_sequen:
                    temp={target_station:[(line,sequen[target_index%len(sequen)])]}
                    station_line_arrive_sequen[start_station]=temp
                    continue
                if target_station not in station_line_arrive_sequen[start_station]:
                    temp=[(line,sequen[target_index%len(sequen)])]
                    station_line_arrive_sequen[start_station][target_station]=temp
                    continue
                temp=(line,sequen[target_index%len(sequen)])
                station_line_arrive_sequen[start_station][target_station].append(temp)
    else:
        for direction_index in range(len(direction_list)):
            direction = direction_list[direction_index]
            inverse_direction = direction_list[(direction_index+1)%len(direction_list)]
            sequen_direction = list(line_direct_seq_stat_nums[line][direction].keys())
            sequen_direction.sort()
            sequen_inverse_direction = list(line_direct_seq_stat_nums[line][inverse_direction].keys())
            sequen_inverse_direction.sort()
            end_station,end_nums = line_direct_seq_stat_nums[line][inverse_direction][sequen_inverse_direction[0]]
            for index in range(len(sequen_direction)):
                start_station= line_direct_seq_stat_nums[line][direction][sequen_direction[index]][0]
                for target_index in range(index+1,len(sequen_direction)):
                    target_station = line_direct_seq_stat_nums[line][direction][sequen_direction[target_index]][0]
                    if start_station not in station_line_arrive_sequen:
                        temp={target_station:[(line,sequen_direction[target_index])]}
                        station_line_arrive_sequen[start_station]=temp
                        continue
                    if target_station not in station_line_arrive_sequen[start_station]:
                        temp=[(line,sequen_direction[target_index])]
                        station_line_arrive_sequen[start_station][target_station]=temp
                        continue
                    temp=(line,sequen_direction[target_index])
                    station_line_arrive_sequen[start_station][target_station].append(temp)
                #print(start_station,end_station)
                if start_station not in station_line_arrive_sequen:
                    temp={end_station:[(line,sequen_inverse_direction[0])]}
                    station_line_arrive_sequen[start_station]=temp
                    continue
                if end_station not in station_line_arrive_sequen[start_station]:
                    temp=[(line,sequen_inverse_direction[0])]
                    station_line_arrive_sequen[start_station][end_station]=temp
                    continue
                temp=(line,sequen_inverse_direction[0])
                station_line_arrive_sequen[start_station][end_station].append(temp)
print("dataframe done")