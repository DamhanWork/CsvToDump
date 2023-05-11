import os
import subprocess
import sys

import pandas as pd
import mysql.connector as mysql

from sqlalchemy import create_engine
from mysql.connector import Error

from dotenv import load_dotenv, find_dotenv

from datetime import datetime


def create_db(name):
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASSWORD')

        conn = mysql.connect(host='localhost', user=db_user, password=db_pass)
        if conn.is_connected():
            cursor = conn.cursor()
            # TODO: Will it always be one DB? Can we supply a db name better without hard-coding? cmdargs?
            cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(name))

    except Error as e:
        print("Error while connecting to MySQL", e)


def create_table(csv_name, csv_data, dbname):
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASSWORD')

        conn = mysql.connect(host='localhost', user=db_user, password=db_pass)
        if conn.is_connected():
            print(csv_name[0:-4].lower())
            alchemy_conn = create_engine("mysql+mysqlconnector://" + db_user + ":" + db_pass + "@localhost/{}".format(dbname))
            csv_data.to_sql('{}'.format(csv_name[0:-4].lower()), alchemy_conn, if_exists='replace')
            alchemy_conn.dispose()
            conn.close()

    except Error as e:
        print("Error while connecting to MySQL", e)


def dump_db(path,dbname):
    db_pass = os.getenv('DB_PASSWORD')
    output_name = 'csv_database_dump_'+ datetime.today().strftime('%Y%m%d_%H%M%S') + '.sql'
    dump_process = subprocess.Popen(
        'mysqldump -h localhost -P 3306 -u root -p' + db_pass + ' -B {} > '.format(dbname) + path + output_name,
        shell=True)
    dump_process.wait()
    subprocess.Popen('sed -i "s/utf8mb4_0900_ai_ci/utf8mb4_general_ci/g" {}'.format(path+output_name) , shell=True)


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    # Check if parameters have been passed.
    if len(sys.argv) > 2:
        input_folder = sys.argv[1]
        db_name = sys.argv[2]

        create_db(db_name)

        # Build our list of files, and then create a table for each.
        csv_files = os.listdir(input_folder)
        for csv in csv_files:
            climate_data = pd.read_csv(input_folder + '/' + csv, index_col=False,
                                       delimiter=',',  encoding = "ISO-8859-1")
            create_table(csv, climate_data, db_name)

        # Creates a folder for the output SQL dump
        # in documents if it doesn't exist already
        # then dumps the db to it.
        docs_path = os.path.expanduser('~/Documents')
        if not os.path.exists(docs_path + '/csvToDump'):
            os.makedirs(docs_path + '/csvToDump')
        dump_path = docs_path + '/csvToDump/'

        dump_db(dump_path, db_name)

    else:
        print("No Input folder supplied or DB Name supplied, exiting..")
        exit()

