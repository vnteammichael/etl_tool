import pandas as pd

def run(df,mysql):


    #your code
    result = df[df['action'] == 'signin'].groupby(['time', 'source', 'agent_line', 'device_info']).agg(
        {
            "user_id":"count"
        }
    )
    result.reset_index(inplace=True)
    result.columns = ["report_date","source","dim1","dim2","num1"]
    result['metric'] = "t1_n1"
    mysql.insert_dataframe("metric_report",result) 
    
    
    
    #save detail
    detail = {
        "metric":"t1_n1",
        "dim1":"agent_line",
        "dim2":"device_info",
        "dim3":"",
        "num1":"count",
        "num2":""
    }
    mysql.insert_detail("detail_metric_report",detail) 

    return