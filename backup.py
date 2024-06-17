def fill_weighted_average_updown(df):
    # 对每个空缺值进行处理
    #filled_df = df.copy()
    filled_df = df
    for col in range(len(df.columns)):
        for i in range(len(df)):
            if pd.isnull(df.iloc[i, col]):
                try:
                    find_row_flag = False
                    column1 = df.iloc[i,:].notnull()
                    for row_index in range(len(df)):
                        if row_index == i:
                            continue
                        else:
                            if df.iloc[row_index, col].notnull().sum()!=0:
                                column2 = df.iloc[row_index,:].notnull()
                                where_column = column1 & column2
                                if sum(where_column[:col])>0:
                                    #取左侧
                                    column_next_idx = where_column[:col][::-1].idxmax()
                                    find_row_flag = True
                                    prev_idx = row_index
                                    break
                                elif sum(where_column[col+1:])>0:
                                    #取右侧
                                    column_next_idx = where_column[col:].idxmax()
                                    find_row_flag = True
                                    prev_idx = row_index
                                    break
                                else:
                                    continue
                            else:
                                continue
                    if find_row_flag:
                        prev_prev_time = time_to_seconds(df.loc[prev_idx, column_next_idx])
                        prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                        this_next_time = time_to_seconds(df.loc[i,column_next_idx])
                        # 计算加权平均
                        filled_time = this_next_time + (prev_next_time - prev_prev_time)
                        filled_df.iloc[i, col]  = seconds_to_time(filled_time)
                    else:
                        continue
                except:
                    continue
    return filled_df

#old
def fill_weighted_average_updown(df):
    # 对每个空缺值进行处理
    #filled_df = df.copy()
    filled_df = df
    for col in range(len(df.columns)):
        for i in range(len(df)):
            if pd.isnull(df.iloc[i, col]):
                try:
                    non_null_indices = df.iloc[:, col].notnull()
                    up_nonzero = df.iloc[:i, col].notnull().sum()
                    down_nonzero = df.iloc[i+1:, col].notnull().sum()
                    if down_nonzero>0:
                        # 下平均
                        # 找到前方的最近两个非空值
                        prev_idx = non_null_indices[i+1:].idxmax()#行，i行和prev_idx行
                        column1 = df.iloc[i,:].notnull()
                        column2 = df.iloc[prev_idx,:].notnull()
                        where_column = column1 & column2
                        if sum(where_column[:col])==0 and sum(where_column[col:])==0:
                            continue
                        elif sum(where_column[:col])>0 and sum(where_column[col:])==0:
                            column_prev_idx = where_column[:col][::-1].idxmax()
                            column_next_idx = column_prev_idx
                        else:
                            column_next_idx = where_column[col:].idxmax()
                            column_prev_idx = column_next_idx
                        if pd.isnull(column_prev_idx):
                            prev_prev_time = time_to_seconds(df.loc[prev_idx, column_next_idx])
                            prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                            this_next_time = time_to_seconds(df.loc[i,column_next_idx])
                        else:
                            prev_prev_time = time_to_seconds(df.loc[prev_idx, column_prev_idx])
                            prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                            this_next_time = time_to_seconds(df.loc[i,column_prev_idx])
                        # 计算加权平均
                        filled_time = this_next_time + (prev_next_time - prev_prev_time)
                        filled_df.iloc[i, col]  = seconds_to_time(filled_time)
                    elif up_nonzero>0 and down_nonzero==0:
                        # 上平均
                        # 找到前方的最近两个非空值
                        prev_idx = non_null_indices[:i][::-1].idxmax()#行，i行和prev_idx行
                        #where_column = df.iloc[[i,prev_idx],:].notnull()
                        #column_flag = where_column.sum(axis=0)
                        column1 = df.iloc[i,:].notnull()
                        column2 = df.iloc[prev_idx,:].notnull()
                        where_column = column1 & column2
                        if sum(where_column[:col])==0 and sum(where_column[col:])==0:
                            continue
                        elif sum(where_column[:col])>0 and sum(where_column[col:])==0:
                            column_prev_idx = where_column[:col][::-1].idxmax()
                            column_next_idx = column_prev_idx
                        else:
                            column_next_idx = where_column[col:].idxmax()
                            column_prev_idx = column_next_idx
                        
                        if pd.isnull(column_prev_idx):
                            prev_prev_time = time_to_seconds(df.loc[prev_idx, column_next_idx])
                            prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                            this_next_time = time_to_seconds(df.loc[i,column_next_idx])
                        else:
                            prev_prev_time = time_to_seconds(df.loc[prev_idx, column_prev_idx])
                            prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                            this_next_time = time_to_seconds(df.loc[i,column_prev_idx])
                        # 计算加权平均
                        filled_time = this_next_time + (prev_next_time - prev_prev_time)
                        filled_df.iloc[i, col]  = seconds_to_time(filled_time)
                    else:
                        continue
                except:
                    continue
    return filled_df

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

def fill_weighted_average_updown(df):
    # 对每个空缺值进行处理
    #filled_df = df.copy()
    for col in range(len(df.columns)):
        for i in range(len(df)):
            if pd.isnull(df.iloc[i, col]):
                try:
                    find_row_flag = False
                    column1 = df.iloc[i,:].notnull()
                    
                    total_column = df.notnull()
                    total_judge = column1&total_column
                    for row_index in range(len(df)):
                        if row_index == i:
                            continue
                        else:
                            if total_column.iloc[row_index,col] and total_judge.iloc[row_index,:].sum()>0:
                                column_next_idx = total_judge.iloc[row_index,:].idxmax()
                                find_row_flag = True
                                prev_idx = row_index
                                break
                            else:
                                continue
                    if find_row_flag:
                        prev_prev_time = time_to_seconds(df.loc[prev_idx, column_next_idx])
                        prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                        this_next_time = time_to_seconds(df.loc[i,column_next_idx])
                        # 计算加权平均
                        filled_time = this_next_time + (prev_next_time - prev_prev_time)
                        df.iloc[i, col]  = seconds_to_time(filled_time)
                    else:
                        continue
                except:
                    continue
    return df