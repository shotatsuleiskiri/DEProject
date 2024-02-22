import yaml
import os
class ReadYaml:



    def __init__(self, path, key):
        self.path = path
        self.key = key
        self.tableType = self.getTableType()
        
    
    # private mthod
    def __getYaml (self):
        script_directory = os.path.dirname(os.path.abspath(__file__))
        yaml_file_path = os.path.join(script_directory, self.path)
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
        return yaml_data[self.key]
    
    def getTSourceTableName(self ):
        return self.__getYaml()['SourceTableName']
    
    def getSourceDBName(self ):
        return self.__getYaml()['SourceDBName']
    
    def getSourceSchema(self ):
        return self.__getYaml()['SourceSchema']
    
    def getTableType(self ):
        return self.__getYaml()['TableType']
    
    def getDestTbaleName(self ):
        return self.__getYaml()['DestTableName']
    
    def getDestDBName(self ):
        return self.__getYaml()['DestDBName']
    
    def getDestSchema(self ):
        return self.__getYaml()['DestSchema']
    
    def getfilterColumn(self ):
        return self.__getYaml()['FilterColumn']
    
    def getCode(self):
        return self.__getYaml()['Code']
    
    def getSurogateKey(self):
        return self.__getYaml()['SurogateKey']

    def getNaturalKey(self):
        return self.__getYaml()['NaturalKey']

    def getInsertionType(self):
        return self.__getYaml()['InsertionType']

    def getRenameColumn(self):
        return self.__getYaml()['RenameColumn']

