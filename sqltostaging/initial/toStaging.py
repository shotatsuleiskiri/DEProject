from myFramework.utils.readYaml import ReadYaml

class ToStaging(ReadYaml):
    
    def __init__(self, path, key):
        self.key = key
        self.path = path
