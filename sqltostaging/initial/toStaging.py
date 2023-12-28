from myFramework.utils.readYaml import ReadYaml
import myFramework.source.posgresql.connect as conn
import pandas as pd
from myFramework.utils.getDF import getDF
from myFramework.utils.fiilStaging import fillStaging
import sys

sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')

class ToStaging( getDF, fillStaging):
    
    def __init__(self, getSourceDBName, getSourceSchema):
        self.getSourceDBName = getSourceDBName
        self.getSourceSchema = getSourceSchema

    def getDF(self,  tablename, schema):
        return pd.read_sql(f"select T.*,  DATE(CURRENT_TIMESTAMP) insertion_date from {schema}.{tablename} T"
                ,conn.getConnection(self.getSourceDBName))
    
    def fillstaging(self, df, dst_dbname, schema, tablename):
        df.to_sql(tablename, conn.getConnection(dst_dbname)
                , schema=f"{schema}", if_exists='replace', index=False)
