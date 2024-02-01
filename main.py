import myFramework.utils.utils as utils
from sqltostaging.initial.toStaging import ToStaging as toStaging_initial
from sqltostaging.full.toStaging import ToStaging as toStaging_full
from sqltostaging.incremental.toStaging import ToStaging as toStaging_incremental
from managment.cleanstaging.cleanStaging import CleanStaging 
from myFramework.utils.readYaml import ReadYaml
from stagingtodv.nodatemanaged.toDV import ToDV


# ---------------------------DV----------------


# Full
# create ReadYaml object

# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/toDV/dvdrental/full.yaml", 'dvdrental.store')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
# ganaretedDF = utils.generateSurogateKey(sourceDF,test.getCode(), test.getNaturalKey() )
# utils.fillPosgres(ganaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())



# incremental
# create ReadYaml object

testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/toDV/dvdrental/incremental.yaml", 'dvdrental.rental')
test = ToDV(testread.path, testread.key)
sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2006-02-14", "2006-02-15")
ganaretedDF = utils.generateSurogateKey(sourceDF,test.getCode(), test.getNaturalKey() )
utils.fillPosgres(ganaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())




                        # S T A G I N G
# # initial
# test = toStaging_initial('dvdrental','public')
# tbname_Df = utils.getTbaleList(test.getSourceDBName,test.getSourceSchema)
# for tbname in tbname_Df['tablename']:    
#     utils.fillPosgres(utils.getDF( tbname ,test.getSourceSchema),'DBStaging','dvdrental', tbname)
  
   
# full load
# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.category')
# test = toStaging_full(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
# utils.fillPosgres(sourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())

# incremental 
# testread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/incremental.yaml", 'public.payment')
# test = toStaging_incremental(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2007-02-10", "2007-05-16")
# utils.fillPosgres(sourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())

# clean staging
# cleantestread = ReadYaml("/Users/ramazkapanadze/DEProject/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.category')
# t = CleanStaging()
# t.cleanStaging('DBStaging', 'dvdrental', 'category', 2023, 12, 28)

