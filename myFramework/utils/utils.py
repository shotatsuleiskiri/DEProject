import myFramework.source.posgresql.connect as conn
import pandas as pd



def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'"
                        ,conn.getConnection(dbname))


def fillstaging(df, dst_dbname, schema, tablename):
        df.to_sql(tablename, conn.getConnection(dst_dbname)
                , schema=f"{schema}", if_exists='replace', index=False)
        


