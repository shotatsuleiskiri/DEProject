from myFramework.utils.readYaml import ReadYaml
import myFramework.source.posgresql.connect as conn
import pandas as pd

class ToStaging(ReadYaml):
    
    def __init__(self, path, key):
        self.key = key
        self.path = path
    


    # def getDF(self, source_dbname, tablename, schema):
    #     return pd.read_sql(f"select T.*,  DATE(CURRENT_TIMESTAMP) insertion_date from {schema}.{tablename} T"
    #             ,conn.getConnection(source_dbname))
    
    # def fillstaging(self, df, dst_dbname, schema, tablename):
    #     df.to_sql(tablename, conn.getConnection(dst_dbname)
    #             , schema=f"{schema}", if_exists='replace', index=False)
