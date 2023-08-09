from clickhouse_driver import Client
import pandas as pd

class ClickHouseConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = None

    def connect(self):
        self.client = Client(host=self.host, port=self.port, user=self.user, password=self.password)

    def execute_query(self, query):
        if not self.client:
            self.connect()

        result = self.client.execute(query)
        return result
    
    def read_query_as_dataframe(self, query, cols, chunk_size=10000):
        if not self.client:
            self.connect()

        offset = 0
        dfs = []

        while True:
            query_with_offset = f'{query} LIMIT {chunk_size} OFFSET {offset}'
            result = self.client.execute(query_with_offset)

            if not result:
                break

            # columns = [col[0] for col in result.cursor.description]
            data = [list(row) for row in result]
            df = pd.DataFrame(data, columns=cols)
            dfs.append(df)

            offset += chunk_size

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return None
        
    def close(self):
        if self.client:
            self.client.disconnect()