#!/bin/python3

import json
import mysql.connector
from mysql.connector import errors
import logging
import import_extractor
from dotenv import load_dotenv
from pathlib import Path
import os

# Load enviroment from .env_pro
env_path = Path('.') / '.env_pro'
load_dotenv(dotenv_path=env_path)


# Insert log to file
logging.basicConfig(filename=os.getenv("log_file") , filemode='w',
                   format='%(asctime)s %(name)s %(levelname)s %(message)s',level=os.getenv("log_level"))

# List for items imported
def import_list(data):
    main_list = []
    key_list = json.loads(os.getenv("item_list"))
    for value in key_list:
        var = eval("import_extractor.%s(data)" % value)
        main_list.append(var)
    zipped = zip(key_list, main_list)
    filter_none = list(filter(lambda x: x[1] is not None, zipped))
    unzippedA = [x[0] for x in filter_none]
    unzippedB = [x[1] for x in filter_none]
    logging.debug(f"import_list: {unzippedA}")
    return unzippedA+unzippedB

# Import query set
def import_query(get_list):
    mean = len(get_list)//2
    table = os.getenv("mysql_table")
    cnt = 0 
    query_item = ""
    query_value = ""
    while mean > cnt:
        meancnt = cnt+mean
        if isinstance(get_list[meancnt], int):
            if cnt+1 == mean :
                query_item += f"{get_list[cnt]}"
                query_value += f"{get_list[meancnt]}"
            else:
                query_item += f"{get_list[cnt]}, "
                query_value += f"{get_list[meancnt]}, "
        else:
            if cnt+1 == mean :
                query_item += f"{get_list[cnt]}"
                query_value += f"'{get_list[meancnt]}'"
            else:
                query_item += f"{get_list[cnt]}, "
                query_value += f"'{get_list[meancnt]}',"
        cnt += 1
    query = f"INSERT INTO {table}({query_item}) VALUES({query_value})"
    return query


# Delete query
def delete_query(get_id):
    table = os.getenv("mysql_table")
    mean = len(get_id)//2
    query = f"delete from `{table}` where `id` = '{get_id[mean]}'"
    return query


# Insert to MySQL
with open(os.getenv("mongo_data_path")) as fp: 
    line = fp.readline()
    cnt = 1 
    myconnection = mysql.connector.connect(host=os.getenv("mysql_host"), user=os.getenv("mysql_user"),
                            passwd=os.getenv("mysql_pass"),
                            database=os.getenv("mysql_db"))

    mycursor = myconnection.cursor()
    while line:
        line = fp.readline()
        json_map = json.loads(line)
        if cnt%20 == 0:
            mycursor.close()
            mycursor = myconnection.cursor()
        val = import_query(import_list(json_map))
        reval = delete_query(import_list(json_map))
        try:
            cnt += 1
            mycursor.execute(val)
            myconnection.commit()
        except mysql.connector.IntegrityError as e:
            logging.info(f"This query {val} is duplicate")
            mycursor = myconnection.cursor()
            mycursor.execute(reval)
            myconnection.commit()
            logging.info(f"This query {val} duplicated is delete")
            mycursor = myconnection.cursor()
            mycursor.execute(val)
            myconnection.commit()
            logging.info(f"This query {val} deleted is duplicate")

