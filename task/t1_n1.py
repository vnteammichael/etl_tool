import pandas as pd


def run(df,mysql):

    try:
        #your code
        result = df[df['action'] == 'register'].groupby(['time', 'source', 'agent_line']).agg(
            {
                "user_id":"count"
            }
        )
        result.reset_index(inplace=True)
        result.columns = ["report_date","source","dim1","num1"]
        result['metric'] = "t1_n1"

        mysql.insert_dataframe("metric_report",result) 
        
        
        
        #save detail
        detail = {
            "metric":"t1_n1",
            "dim1":"agent_line",
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