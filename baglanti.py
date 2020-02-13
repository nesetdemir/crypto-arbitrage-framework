#!/usr/bin/python
import requests
import json
import mysql.connector
from mysql.connector import Error
 
def mysql_baglan(dbname):
    try:
        mySQLconnection = mysql.connector.connect(host="localhost",  # your host
                                                  user="root",       # username
                                                  passwd="72021112*k",     # password
                                                  db=dbname)   # name of the database

        return mySQLconnection
    except Error as e:
        print("Error while connecting to MySQL", e)