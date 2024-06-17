def fill_weighted_average_updown(df):
    # 对每个空缺值进行处理
    #filled_df = df.copy()
    filled_df = df
    for col in range(len(df.columns)):
        for i in range(len(df)):
            if pd.isnull(df.iloc[i, col]):
                try:
                    non_null_indices = df.iloc[:, col].notnull()
                    column1 = df.iloc[i,:].notnull()
                    total_column = df.notnull()
                    result_indices = ((column1 & total_column).sum(axis=1) >0) & non_null_indices
                    if result_indices.sum() ==0:
                        continue
                    prev_idx = result_indices.idxmax()
                    column_next_idx = (column1 & total_column).iloc[i,:].idxmax()
                    prev_prev_time = time_to_seconds(df.loc[prev_idx, column_next_idx])
                    prev_next_time = time_to_seconds(df.iloc[prev_idx, col])
                    this_next_time = time_to_seconds(df.loc[i,column_next_idx])
                    # 计算加权平均
                    filled_time = this_next_time + (prev_next_time - prev_prev_time)
                    filled_df.iloc[i, col]  = seconds_to_time(filled_time)
                except:
                    continue
    return filled_df