import yaml
from db_util.mysql import MySQLConnector
from db_util.clickhouse import ClickHouseConnector 
import pandas as pd
import argparse
from datetime import date
from datetime import timedelta
import os

from task import *

def init_param():
    parser = argparse.ArgumentParser(description ='ETL _tool')
 
    parser.add_argument('-d', '--date', dest ='date',
                        help ='target date')
    
    parser.add_argument('-m', '--mode', dest ='mode', help ='Mode')
    args = parser.parse_args()
    return args

script_dir = os.path.dirname(os.path.abspath(__file__))
data_file_path = os.path.join(script_dir, 'config.yaml')

def read_config_from_file(filename: str = data_file_path):
    try:
        with open(filename, "r") as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        print(f"Error reading configuration: {str(e)}")

# Read the configuration from the file



def main():
    args = init_param()

    today = date.today()
    yesterday = today - timedelta(days = 1) if args.date is None else args.date 
    mode = "all" if args.mode is None else args.mode

    
    
    config = read_config_from_file()
    mysql = MySQLConnector(**config['mysql'])
    clickhouse = ClickHouseConnector(**config['clickhouse'])
    cols = ["time", "source", "action", "agent_code", "user_agent_data", "device_info", "os_info", "ip_address", "latitude", "longitude", "country", "region", "region_code", "city", "user_id", "phone", "email"]
    query = f"SELECT {' ,'.join(cols)} FROM {config['clickhouse']['database']}.{config['table']['log']} WHERE toDate(time,'Asia/Hanoi') = '{yesterday}' "

    result = clickhouse.read_query_as_dataframe(query,cols =cols)
    # result['time'] = result['time'].dt.tz_convert('Asia/Ho_chi_minh')
    result['time'] = yesterday


    agent_mapping  = clickhouse.read_query_as_dataframe("SELECT agent_line,agent_code FROM data_tracking.agent_mapping",cols =['agent_line','agent_code'])
    # print(agent_mapping)
    result = result.merge(agent_mapping,how='left',on='agent_code')

    #delete duplicated
    mysql.delete_data_by_condition(table="metric_report",condition_dict={"report_date":yesterday})



    t1_n1.run(result,mysql)
    t2_new_user_per_ip.run(result,mysql)
    t3_new_user_per_locate.run(result,mysql)
    t4_a1.run(result,mysql)
    t5_a1_per_device.run(result,mysql)
    t6_a1_per_country.run(result,mysql)
    t7_a1_per_region.run(result,mysql)
    t8_a1_per_city.run(result,mysql)
    t9_top_login_ip.run(result,mysql)
    t10_n1_per_device.run(result,mysql)

    return




if __name__ == "__main__":
    
    main()
