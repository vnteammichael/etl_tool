import pandas as pd

def run(df,mysql):

    try:
        #your code
        result = df[df['action'] == 'login'].groupby(['time', 'source', 'region', 'region_code']).agg(
            {
                "user_id":"count"
            }
        )
        result.reset_index(inplace=True)
        result.columns = ["report_date","source","dim1","dim2","num1"]
        result['metric'] = "t7_a1_per_region"
        mysql.insert_dataframe("metric_report",result) 
        
        
        
        #save detail
        detail = {
            "metric":"t7_a1_per_region",
            "dim1":"region",
            "dim2":"region_code",
            "dim3":"",
            "num1":"count distinct",
            "num2":""
        }
        mysql.insert_detail("detail_metric_report",detail) 
    except Exception as ex:
        print(f"Error {ex}")
        return False

    return True