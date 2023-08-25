import pandas as pd


def run(df,mysql):

    try:
        #your code
        df['device_info'] = df['device_info'].map(lambda x: x.lower())
        result = df[df['action'] == 'register'].groupby(['time', 'source', 'device_info']).agg(
            {
                "user_id":"count"
            }
        )
        result.reset_index(inplace=True)
        result.columns = ["report_date","source","dim1","num1"]
        result['metric'] = "t10_n1_per_device"

        mysql.insert_dataframe("metric_report",result) 
        
        
        
        #save detail
        detail = {
            "metric":"t10_n1_per_device",
            "dim1":"device_info",
            "dim2":"",
            "dim3":"",
            "num1":"count",
            "num2":""
        }
        mysql.insert_detail("detail_metric_report",detail)
    except Exception as ex:
        print(f"Error {ex}")
        return False
    return True