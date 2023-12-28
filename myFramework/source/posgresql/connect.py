from sqlalchemy import create_engine


def getConnection(dbname):
    user='ramazkapanadze'
    password='1604'
    host ='localhost'
    port='5432'
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')   

