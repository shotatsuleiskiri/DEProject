from myFramework.utils.readYaml import ReadYaml
import myFramework.source.posgresql.connect as conn
import pandas as pd


class ToDV(ReadYaml):
    
    def __init__(self, path, key):
        self.key = key
        self.path = path

    