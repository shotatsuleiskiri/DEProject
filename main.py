import myFramework.utils.utils as utils
from sqltostaging.initial.toStaging import ToStaging as toStaging_initial
from sqltostaging.full.toStaging import ToStaging as toStaging_full
from sqltostaging.incremental.toStaging import ToStaging as toStaging_incremental
from myFramework.utils.readYaml import ReadYaml



# initial
# test = toStaging_initial(testread.path, testread.key)
# tbname_Df = utils.getTbaleList(test.getSourceDBName(),test.getSourceSchema())
# for tbname in tbname_Df:    
#     utils.fillstaging(utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema()),test.getDestDBName(),test.getDestSchema())
   
   
# full load
testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.category')
test = toStaging_full(testread.path, testread.key)
sourceDF = test.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
test.fillstaging(sourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())


# incremental 

# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/incremental.yaml", 'public.payment')
# test = toStaging_incremental(testread.path, testread.key)
# sourceDF = test.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2007-02-10", "2007-05-16")
# test.fillstaging(sourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())

# print(sourceDF)
# print(f'"{test.getDestDBName()}"')
# print(f'"{test.getDestSchema()}"')
# print(test.getDestTbaleName())