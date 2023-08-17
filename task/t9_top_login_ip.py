import pandas as pd

def run(df,mysql):

    try:
        #your code
        result = df[df['action'] == 'login'].groupby(['time', 'source', 'ip_address']).agg(
            {
                "user_id":"count"
            }
        )
        result.reset_index(inplace=True)
        result.columns = ["report_date","source","dim1","num1"]
        result['metric'] = "t9_top_login_ip"
        mysql.insert_dataframe("metric_report",result) 
        
        
        
        #save detail
        detail = {
            "metric":"t9_top_login_ip",
            "dim1":"ip_address",
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