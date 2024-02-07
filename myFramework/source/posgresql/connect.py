from sqlalchemy import create_engine


def getConnection(dbname):
    user='mariammakharadze'
    password='guguna1414'
    host ='localhost'
    port='5432'
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')  

