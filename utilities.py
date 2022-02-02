# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 16:19:30 2020

Basic SQL commands in one line of code

@author: pcooper
"""

import pandas as pd
import re
import requests
from sqlalchemy import create_engine
from bs4 import BeautifulSoup


# connect and write to SQL
class SQLServer:            ### add error handling - if operation times out once, try again
    """
    connect to SQL environment and perform common actions
    """
    
    dialect = 'mssql+pyodbc'
    driver = 'SQL+Server+Native+Client+11.0'
    
    def __init__(self, server, database, user='', password=''):
        self.server = server
        self.database = database
        self.user_pass = ''
        if user and password: self.user_pass = f'{user}:{password}'
        self.con_string = (f'{self.dialect}://{self.user_pass}@{self.server}/'
                           f'{self.database}?driver={self.driver}')
        self.engine = create_engine(self.con_string)
    
    def execute(self, file):
        with open(file, 'r') as sql_file, self.engine.connect() as con:
            con.execute(sql_file.read())
    
    def read(self, file):
        with open(file, 'r') as sql_file, self.engine.connect() as con:
            return pd.read_sql_query(sql_file.read(), con)
        
    def read_table(self, table, head=None):
        query = f'SELECT * FROM {table}'
        if head:
            query = f'SELECT TOP {head} * FROM {table}'
        with self.engine.connect() as con:
            return pd.read_sql_query(query, con)
        
    def write(self, df, name, schema=None): 
        df.to_sql(name=name, con=self.engine, schema=schema,
                       if_exists='fail', index=False)
        
    def overwrite(self, df, name, schema=None): 
        df.to_sql(name=name, con=self.engine, schema=schema,
                       if_exists='replace', index=False)


def print_sql(file):
    """
    read sql file and print to console
    """
    with open(file, 'r') as sql_file:
        print(sql_file.read())


def dd_preprocess(x):
    """
    Processing function for strings, to make it easier for dedupe to
    consume
    """
    x = str(x)
    x = re.sub('\n', ' ', x)
    x = re.sub('-', '', x)
    x = re.sub('/', ' ', x)
    x = re.sub("'", '', x)
    x = re.sub(",", '', x)
    x = re.sub(":", ' ', x)
    x = re.sub('  +', ' ', x)
    x = x.strip().strip('"').strip("'").lower().strip()
    if not x:
        x = None
    return x


def soupify(url):
    """
    Return BeautifulSoup html object from a URL
    """
    r = requests.get(url)
    return BeautifulSoup(r.content, 'html.parser')