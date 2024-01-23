import myFramework.source.posgresql.connect as conn
import pandas as pd
from multipledispatch import dispatch


# import sys
# sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')


def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'", conn.getConnection(dbname))


def posgreExecute(dbName, query):
        engine = conn.getConnection(dbName)
        engine.execute(query)



@dispatch(str)
def getDF( test):
    return test

@dispatch(str, str, str)
def getDF( source_dbname, tablename, schema):
    query = f"select T.*,  DATE(CURRENT_TIMESTAMP) insertion_date from {schema}.{tablename} T"
    return pd.read_sql(query ,conn.getConnection(source_dbname))

@dispatch(str, str, str, str, str)
def getDF( source_dbname, tablename,schema,filterColumn, dateFrom, dateTo):
    query = f"select T.*, DATE(CURRENT_TIMESTAMP) insertion_date from {schema}.{tablename} T where {filterColumn} >= '{dateFrom}' and {filterColumn} < '{dateTo}' "
    return pd.read_sql(query ,conn.getConnection(source_dbname))

   
