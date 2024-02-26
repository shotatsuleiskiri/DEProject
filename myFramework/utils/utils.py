import myFramework.source.posgresql.connect as conn
import pandas as pd
from multipledispatch import dispatch
from datetime import datetime
from pandas_scd import scd2


# import sys
# sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')


def getTbaleList(dbname, schema):
        return pd.read_sql(f"select tablename  from pg_catalog.pg_tables where schemaname = '{schema}'", conn.getConnection(dbname))


def posgreExecute(dbName, query):
        engine = conn.getConnection(dbName)
        engine.execute(query)


@dispatch(str, str, str)
def getDF( source_dbname, tablename, schema):
    query = f"select T.* from {schema}.{tablename} T"
    return pd.read_sql(query ,conn.getConnection(source_dbname))

# @dispatch(str, str, str, str, str)
# def getDF( source_dbname, tablename,schema,filterColumn, dateFrom):
#     query = f"select T.* from {schema}.{tablename} T where {filterColumn} >= '{dateFrom}'"
#     return pd.read_sql(query ,conn.getConnection(source_dbname))

@dispatch(str, str, str, str, str,str)
def getDF( source_dbname, tablename,schema,filterColumn, dateFrom, dateTo):
    query = f"select T.* from {schema}.{tablename} T where {filterColumn} >= '{dateFrom}' and {filterColumn} < '{dateTo}' "
    return pd.read_sql(query ,conn.getConnection(source_dbname))

def addInsertionDate(df: pd.DataFrame ):
      return df.assign(insertion_date = lambda x: datetime.now())


# def generateSurogateKey(df, code, SurogatekeyList):
#       newdf = pd.DataFrame(df)
#       col_list = list(newdf.columns)
#       for key in SurogatekeyList:
#         Surogatekey = newdf[key]+int(str(1000000) + str(code))
#         newdf = newdf.assign(tmpkey = pd.Series(Surogatekey).values).drop(f'{key}', axis=1)
#         newdf.rename(columns={'tmpkey':f'gen_{key}'}, inplace=True)
#         col_list.remove(key)
#         col_list.insert(0,f"gen_{key}")
#       newdf = newdf.reindex(col_list, axis=1)
#       return newdf

def generateSurogateKey(df, code, SurogatekeyList, dest_col_list):
      newdf = pd.DataFrame(df)
      for key in SurogatekeyList:
        Surogatekey = newdf[key]+int(str(1000000) + str(code))
        newdf = newdf.assign(tmpkey = pd.Series(Surogatekey).values).drop(f'{key}', axis=1)
        newdf.rename(columns={'tmpkey':f'gen_{key}'}, inplace=True)
      newdf = newdf.reindex(dest_col_list, axis=1)
      return newdf

def GenerateNaturalKey(df, Naturalkey):
    newdf = pd.DataFrame(df)
    NaturalValue = newdf[Naturalkey]
    newdf = newdf.assign(tmpkey = pd.Series(NaturalValue).values)
    newdf.rename(columns={'tmpkey':f'source_{Naturalkey}'}, inplace=True)
    return newdf






def fillPosgres( df, dst_dbname, schema, tablename, insertiontype):
        df.to_sql(tablename, conn.getConnection(dst_dbname)
                , schema=f"{schema}", if_exists=insertiontype, index=False)


def toSCD2(srcDF: pd.DataFrame, targetDF: pd.DataFrame, dest_col_list):
      for i in dest_col_list:
          targetDF.rename(columns={i}, inplace=True)
      final_df = scd2(srcDF, targetDF)
      return final_df




# def column_row_count(df):
#   num_columns = df.shape[1]
#   print(num_columns)
#   num_rows = df.shape[0]
#   total = num_columns * num_rows
#   return total
#
#
# def addcolumn(df, source_dbname, dst_dbname):
#     return df.assign(MovedFrom = lambda x: f'From {source_dbname}  To {dst_dbname}')



