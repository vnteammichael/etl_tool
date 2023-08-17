import pandas as pd

def run(df,mysql):

    try:
        #your code
        result = df[df['action'] == 'login'].groupby(['time', 'source', 'country']).agg(
            {
                "user_id":"nunique"
            }
        )
        result.reset_index(inplace=True)
        result.columns = ["report_date","source","dim1","num1"]
        result['metric'] = "t6_a1_per_country"
        mysql.insert_dataframe("metric_report",result) 
        
        
        
        #save detail
        detail = {
            "metric":"t6_a1_per_country",
            "dim1":"country",
            "dim2":"",
            "dim3":"",
            "num1":"count distinct",
            "num2":""
        }
        mysql.insert_detail("detail_metric_report",detail) 
    except Exception as ex:
        print(f"Error {ex}")
        return False

    return True