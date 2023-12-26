import myFramework.source.posgresql.connect as conn
import pandas as pd



def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'"
                        ,conn.getConnection(dbname))

