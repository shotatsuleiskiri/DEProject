from myFramework.utils.readYaml import ReadYaml
import myFramework.source.posgresql.connect as conn
import pandas as pd
# from myFramework.utils.getDF import getDF
# from myFramework.utils.fiilStaging import fillStaging
import sys

sys.path.insert(0, '/Users/ramazkapanadze/DEProject/DEProject/myFramework/source/posgresql')

class ToStaging():
    
     def __init__(self, getSourceDBName, getSourceSchema):
        self.getSourceDBName = getSourceDBName
        self.getSourceSchema = getSourceSchema

