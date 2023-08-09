import pandas as pd

def run(df,mysql):


    #your code
    result = df[df['action'] == 'create'].groupby(['time', 'source', 'country', 'region', 'city']).agg(
        {
            "user_id":"count"
        }
    )
    result.reset_index(inplace=True)
    result.columns = ["report_date","source","dim1","dim2","dim3","num1"]
    result['metric'] = "t3_new_user_per_locate"
    mysql.insert_dataframe("metric_report",result) 
    
    
    
    #save detail
    detail = {
        "metric":"t3_new_user_per_locate",
        "dim1":"country",
        "dim2":"region",
        "dim3":"city",
        "num1":"count",
        "num2":""
    }
    mysql.insert_detail("detail_metric_report",detail) 

    return