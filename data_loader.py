import cx_Oracle as ora
import json
from config import config_dict, data_path
from datetime import datetime
import time



class OraHandler():

    insert_statement = """insert into taxi_traffic
                (hvfhs_license_num,
                dispatching_base_num,
                pickup_datetime,
                dropoff_datetime,
                pulocationid,
                dolocationid,
                sr_flag, 
                dummy
                )
                values
                (:1, :2, :3, :4, :5, :6, :7, :8)"""

    def __init__(self):
        ora.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_3")   
    
    def connect(self):
        return ora.connect(user=config_dict['username'], password=config_dict['password'],\
        dsn=config_dict['tns_alias'], encoding="UTF-8")

    def bulk_insert(self, data):
        # execute many
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(self.insert_statement, data)
            conn.commit()

    def insert_one_method(self, data):
        # execute many
        with self.connect() as conn:
            with conn.cursor() as cur:
                for item in data:
                    cur.execute(self.insert_statement, item)
            conn.commit()

    def select_data(self):
        data = None
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute('select * from taxi_traffic where rownum < 1001')

                data = cur.fetchall()
        return data


class JSONHandler:

    def read_json_data(self):
        data = None
        with open(data_path, "r") as f:
            data = json.load(f)

        return data

    def make_data_structure(self):
        data = self.read_json_data()
        data_list = []

        for key, value in data['hvfhs_license_num'].items():           
            data_list.append((value, data['dispatching_base_num'][key],\
                    datetime.strptime(data['pickup_datetime'][key], '%Y-%m-%d %H:%M:%S'),\
                    datetime.strptime(data['dropoff_datetime'][key], '%Y-%m-%d %H:%M:%S'), \
                    data['PULocationID'][key], data['DOLocationID'][key], data['PULocationID'][key], data['SR_Flag'][key]
            ))
        return data_list


if __name__ == '__main__':
    temp = OraHandler()
    conn = temp.connect()
    cursor = conn.cursor()


    js_handler = JSONHandler()
    data = js_handler.make_data_structure()

    #temp.bulk_insert(data)

    #temp.insert_one_method(data[0:1000])

    #print(temp.select_data())


    array_sizes = (10000, 1000, 100, 10)
    for size in array_sizes:
        cursor = conn.cursor()
        cursor.arraysize = size
        start = time.time()
        cursor.execute('select * from taxi_traffic where rownum < 10000').fetchall()
        elapsed = time.time() - start
        print("Time for", size, elapsed, "seconds")