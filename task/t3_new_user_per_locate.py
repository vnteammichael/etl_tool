import pandas as pd

def run(df,mysql):

    try:
        #your code
        result = df[df['action'] == 'register'].groupby(['time', 'source', 'country', 'region', 'region_code']).agg(
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
            "dim3":"region_code",
            "num1":"count",
            "num2":""
        }
        mysql.insert_detail("detail_metric_report",detail) 
    except Exception as ex:
        print(f"Error {ex}")
        return False

    return True