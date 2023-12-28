from sqlalchemy import create_engine

user='ramazkapanadze'
password='1604'
host ='localhost'
port='5432'

def getConnection(dbname):
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')   

