import myFramework.source.posgresql.connect as conn
import pandas as pd
from sqlalchemy import create_engine

# import sys

# sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')

user='ramazkapanadze'
password='1604'
host ='localhost'
port='5432'

def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'"
                        ,conn.getConnection(dbname))



def posgreExecute(dbName, query):
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbName}')   
        engine.execute(query)

