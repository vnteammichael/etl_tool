import pandas as pd

def run(df,mysql):


    #your code
    
    
    
    
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