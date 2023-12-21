from myFramework.utils.readYaml import ReadYaml
import myFramework.source.posgresql.connect as conn
import pandas as pd
class ToStaging(ReadYaml):
    
    def __init__(self, path, key):
        self.key = key
        self.path = path

    def getDF(self, source_dbname, tablename, schema):
        return pd.read_sql(f"select * from {schema}.{tablename}"
                ,conn.getConnection(source_dbname))
