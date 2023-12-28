import myFramework.source.posgresql.connect as conn
import pandas as pd

# import sys
# sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')


def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'"
                        ,conn.getConnection(dbname))


def posgreExecute(dbName, query):
        engine = conn.getConnection(dbName)
        engine.execute(query)