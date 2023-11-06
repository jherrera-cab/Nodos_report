    
def df_to_list(df):


    table = []
    for index, row in df.iterrows():
        row_dict = {}
        for column in df.columns:
            row_dict[column] = row[column]
        table.append(row_dict)
    return table


        
    
        