from myFramework.utils.readYaml import ReadYaml
import myFramework.source.posgresql.connect as conn
import pandas as pd
from myFramework.utils.getDF import getDF
from myFramework.utils.fiilStaging import fillStaging
import sys

sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')


class ToStaging(ReadYaml, getDF, fillStaging):
    
    def __init__(self, path, key):
        self.key = key
        self.path = path
    
    def getDF(self, source_dbname, tablename,schema,filterColumn, dateFrom, dateTo):
         return pd.read_sql(f"select * from {schema}.{tablename} where {filterColumn} >= '{dateFrom}' and {filterColumn} < '{dateTo}' "
                ,conn.getConnection(source_dbname))
    
    def fillstaging(self, df, dst_dbname, schema, tablename):
        df.to_sql(tablename, conn.getConnection(dst_dbname)
                , schema=f"{schema}", if_exists='replace', index=False)

