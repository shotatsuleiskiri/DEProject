import myFramework.source.posgresql.connect as conn
import pandas as pd
from multipledispatch import dispatch
from pandas_scd import scd2
from datetime import datetime
import pytz
import duckdb

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
#     query = f"select T.* from {schema}.{tablename} T where {filterColumn} >= '{dateFrom}'  "
#     return pd.read_sql(query ,conn.getConnection(source_dbname))

@dispatch(str, str, str, str, str,str)
def getDF( source_dbname, tablename,schema,filterColumn, dateFrom, dateTo):
    query = f"select T.* from {schema}.{tablename} T where {filterColumn} >= '{dateFrom}' and {filterColumn} < '{dateTo}' "
    return pd.read_sql(query ,conn.getConnection(source_dbname))

def addInsertionDate(df: pd.DataFrame ):
      return df.assign(insertion_date = lambda x: datetime.now())


def generateSurogateKey(df, code, SurogatekeyList, dest_col_list):
      newdf = pd.DataFrame(df)
      for key in SurogatekeyList:
        # print(key)
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

def toSCD2(srcDF: pd.DataFrame, targetDF: pd.DataFrame):
      final_df = scd2(srcDF, targetDF)
      return final_df

def create_time_parts(tz):
    if tz:
        tz = pytz.timezone(tz)
    now = datetime.now(tz)
    now_parts = [str(i) for i in[now.year, now.month, now.day, now.hour, now.minute, now.second]]
    return ",".join(now_parts)

def scd2(src: pd.DataFrame, tgt: pd.DataFrame, cols_to_track: list=None, tz: str=None) -> pd.DataFrame:



    if not cols_to_track:
        cols = list(src.columns)
        cols.remove('last_update')
        cols_to_track = list(cols)
        # print(cols_to_track)

    if not isinstance(cols_to_track, list):
        raise TypeError('cols_to_track must be of type list')

    duck = duckdb.connect()
    duckdb.default_connection.execute("SET GLOBAL pandas_analyze_sample=100000")

    time_parts = create_time_parts(tz)

    duck.execute(f"CREATE TABLE source_scd AS SELECT *, md5(concat_ws('_', {','.join(cols_to_track)})) as hash FROM src")
    duck.execute(f"CREATE TABLE target_scd as select *, md5(concat_ws('_', {','.join(cols_to_track)})) as hash from tgt")

    results = {}

    # rows that are in both target and source, and active
    ### unchanged_active_keys (active - old) ###
    duck.execute("""
        CREATE TABLE unchanged_active_keys as 
        select * 
        from target_scd
        where is_active = True
        and exists(
            select * 
            from source_scd
            where target_scd.hash = source_scd.hash
        )
        """)

    unchanged_active_keys = duck.execute("select count(*) from unchanged_active_keys").fetchone()[0]
    results['unchanged active keys (active - old)'] = unchanged_active_keys

     # rows that are in the target but no longer in the source, and are inactive
    ### unchanged_inactive_keys (inactive - old) ###
    duck.execute("""
        CREATE TABLE unchanged_inactive_keys as 
        select * 
        from target_scd
        where is_active = False
        and not exists(
            select * 
            from source_scd
            where target_scd.hash = source_scd.hash
        )
        """)

    unchanged_inactive_keys = duck.execute("select count(*) from unchanged_inactive_keys").fetchone()[0]
    results['unchanged inactive keys (inactive - old)'] = unchanged_inactive_keys

     # rows that are in the target but no longer in the source, and are active
    ### ended_keys (inactive - new) ###

    duck.execute(f"""
    create table ended_keys as 
    select * EXCLUDE (end_ts, is_active, hash),
    current_timestamp as end_ts,
    0 as is_active,
    from target_scd
    where is_active = True
    and not exists (
        select * 
        from source_scd
        where target_scd.hash = source_scd.hash
    )
    """)


    ended_keys = duck.execute("select count(*) from ended_keys").fetchone()[0]
    results['ended keys (inactive - new)'] = ended_keys

    # rows that are in the source but not in the target
    ### new_keys (active - new) ###
    duck.execute(f"""
    create table new_keys as 
    select * EXCLUDE (hash),
    current_timestamp as start_ts,
    null as end_ts,
    1 as is_active,
    from source_scd
    where not exists (
        select * 
        from target_scd
        where target_scd.hash = source_scd.hash
    )
    """)

    new_keys = duck.execute("select count(*) from new_keys").fetchone()[0]
    results['new keys (active - new)'] = new_keys

    duck.execute("""
    create table final as 
    select * EXCLUDE(hash) from unchanged_active_keys
    union all by name 
    select * EXCLUDE(hash) from unchanged_inactive_keys
    union all by name 
    select * from ended_keys   
   -- union all by name 
   -- select * from new_keys
    """)



    # print(duck.execute("select * from unchanged_active_keys").fetch_df())
    # print('unchanged_active_keys')
    # print(duck.execute("select * from unchanged_inactive_keys").fetch_df())
    # print('unchanged_inactive_keys')
    # print(duck.execute("select * from ended_keys").fetch_df())
    # print('ended_keys')
    print(duck.execute("select * from new_keys").fetch_df())
    print('new_keys')


    # print(results)

    # duck.execute("select * from unchanged_active_keys").fetch_df()


    df = duck.execute("select * from final").fetch_df()
    # .drop("last_update",  axis=1)

    return df


def scdtest(source_df, target_df, cols_to_gen, naturalkey, cols_to_track: list=None):

    for col in cols_to_gen:
         target_df.rename(columns={f'gen_{col}':col}, inplace=True)

    if not cols_to_track:
        cols = list(source_df.columns)
        cols.remove(naturalkey)
        cols_to_track = list(cols)

    source_df[f'source_{naturalkey}'] =  source_df[naturalkey]
    # print(source_df)


#  Check if target data is empty (initial load)
    if target_df.empty:
        # Add "start_date" column with current date for initial load
        source_df["start_ts"] = pd.to_datetime("today")
        source_df["end_ts"] = pd.NA
        # Copy source data to target data
        target_df = source_df
        target_df[f'source_{naturalkey}'] = source_df[naturalkey]

    else:
        # define new records in source df and add start date
        new_df = source_df[~source_df[naturalkey].isin(target_df[f'source_{naturalkey}'])]
        new_df["start_ts"] = pd.to_datetime("today")

        # print('new_df')
        # print(new_df)

        # define existing record in source df
        old_df = source_df[source_df[naturalkey].isin(target_df[f'source_{naturalkey}'])]

        # print('old df')
        # print(old_df)

        # define updated rows ins target df
        merge_df = old_df.merge(target_df, on=cols_to_track, how='outer', indicator=True)
        # print(merge_df)
        diff_in_old_df_df = merge_df[merge_df['_merge']=='left_only']
        diff_in_old_df_df["start_ts"] = pd.to_datetime("today")
        data_diff_in_old_df = diff_in_old_df_df.drop(columns=['_merge', f'source_{naturalkey}_y',f'{naturalkey}_y'])
        data_diff_in_old_df.rename(columns={f'source_{naturalkey}_x':f'source_{naturalkey}'}, inplace=True)
        data_diff_in_old_df.rename(columns={f'{naturalkey}_x':f'{naturalkey}'}, inplace=True)

        # print('data_diff_in_old_df')
        # print(data_diff_in_old_df)


        # Update existing records in target data is source df exist change records
        for index, row in data_diff_in_old_df.iterrows():
            target_df.loc[target_df[f'source_{naturalkey}'] == row[naturalkey], "end_ts"] = pd.to_datetime("today")
        
        # Concatenate updated and new data to target data
        target_df = pd.concat([target_df,data_diff_in_old_df,new_df ], ignore_index = True)

        # print("last target_df")
        # print(target_df)

    # Print the final target DataFrame
    return target_df
