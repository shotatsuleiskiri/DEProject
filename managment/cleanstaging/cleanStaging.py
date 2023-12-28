from myFramework.utils.readYaml import ReadYaml
from sqlalchemy import create_engine
from datetime import date
from myFramework.utils.utils import posgreExecute
# import pandas as pd
import myFramework.source.posgresql.connect as conn
# import sys

# sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')

# user='ramazkapanadze'
# password='1604'
# host ='localhost'
# port='5432'


class CleanStaging(ReadYaml):

    def __init__(self) -> None:
        pass

    def cleanStaging(self, dbName, schemaName, TableName, dateYear, dateMonth, date_day):
        # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbName}')   
        dates = date(dateYear, dateMonth, date_day)
        posgreExecute(dbName,f"delete from {schemaName}.{TableName} where insertion_date <= '{dates}' ")
        # engine.execute(f"delete from {schemaName}.{TableName} where insertion_date <= '{dates}' ")
      