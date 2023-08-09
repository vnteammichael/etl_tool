import mysql.connector
import pandas as pd

class MySQLConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def insert_dataframe(self, table, dataframe, igorne = False):
        if not self.connection:
            self.connect()

        dataframe_columns = list(dataframe.columns)
        placeholders = ', '.join(['%s'] * len(dataframe_columns))

        query = f"INSERT "
        query += " IGNORE "  if igorne else ""  
        query += f" INTO {table} ({', '.join(dataframe_columns)}) VALUES ({placeholders})"
        
        cursor = self.connection.cursor()
        for row in dataframe.itertuples(index=False):
            cursor.execute(query, row)
        self.connection.commit()
        cursor.close()
    
    def delete_data_by_condition(self, table, condition_dict):
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()
        placeholders = ', '.join([f'{key} = %s' for key in condition_dict.keys()])
        values = tuple(condition_dict.values())
        query = f"DELETE FROM {table} WHERE {placeholders}"
        cursor.execute(query, values)
        self.connection.commit()
        cursor.close()

    def insert_detail(self, table, detail):
        if not self.connection:
            self.connect()

        dataframe_columns = list(detail.keys())
        placeholders = ', '.join(['%s'] * len(dataframe_columns))

        query = f"INSERT IGNORE  INTO {table} ({', '.join(dataframe_columns)}) VALUES ({placeholders})"
        
        cursor = self.connection.cursor()
        cursor.execute(query, list(detail.values()))
        self.connection.commit()
        cursor.close()
    

    def close(self):
        if self.connection:
            self.connection.close()