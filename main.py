import yaml
from db_util.mysql import MySQLConnector
from db_util.clickhouse import ClickHouseConnector 
import pandas as pd
import argparse
from datetime import datetime

from task import *

def init_param():
    parser = argparse.ArgumentParser(description ='ETL _tool')
 
    parser.add_argument('-d', '--date', dest ='date',
                        help ='target date')
    
    parser.add_argument('-m', '--mode', dest ='mode', help ='Mode')
    args = parser.parse_args()
    return args

def read_config_from_file(filename: str = "config.yaml"):
    try:
        with open(filename, "r") as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        print(f"Error reading configuration: {str(e)}")

# Read the configuration from the file



def main():
    args = init_param()

    date = datetime.now().strftime("%Y-%m-%d") if args.date is None else args.date 
    mode = "all" if args.mode is None else args.mode

    
    
    config = read_config_from_file()
    mysql = MySQLConnector(**config['mysql'])
    clickhouse = ClickHouseConnector(**config['clickhouse'])
    cols = ["time", "source", "action", "agent_line", "user_agent_data", "device_info", "os_info", "ip_address", "latitude", "longitude", "country", "region", "city", "user_id", "phone", "email"]
    query = f"SELECT {' ,'.join(cols)} FROM {config['clickhouse']['database']}.LogTracking "

    result = clickhouse.read_query_as_dataframe(query,cols =cols)
    result['time'] = pd.to_datetime(result["time"]).dt.date

    #delete duplicated
    mysql.delete_data_by_condition(table="metric_report",condition_dict={"report_date":date})

    t1_n1.run(result,mysql)
    t2_new_user_per_ip.run(result,mysql)
    t3_new_user_per_locate.run(result,mysql)


    return




if __name__ == "__main__":
    
    main()
