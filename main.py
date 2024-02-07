import myFramework.utils.utils as utils
from sqltostaging.initial.toStaging import ToStaging as toStaging_initial
from sqltostaging.full.toStaging import ToStaging as toStaging_full
from sqltostaging.incremental.toStaging import ToStaging as toStaging_incremental
from managment.cleanstaging.cleanStaging import CleanStaging 
from myFramework.utils.readYaml import ReadYaml
from stagingtodv.nodatemanaged.toDV import ToDV


# ---------------------------DV----------------


# Full - SCDType1
# create ReadYaml object

# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/toDV/dvdrental/SCDType1.yaml", 'dvdrental.language')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
# ganaretedDF = utils.generateSurogateKey(sourceDF,test.getCode(), test.getNaturalKey() )
# utils.fillPosgres(ganaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())
#

# incremental
# create ReadYaml object

# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/toDV/dvdrental/incremental.yaml", 'dvdrental.film_category')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2007-02-14", "2006-04-30").drop("insertion_date",  axis=1)
# genaretedDF = utils.generateSurogateKey(sourceDF,test.getCode(), test.getNaturalKey() )
# # print(genaretedDF)
# utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())
#

# SCDType2

testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/toDV/dvdrental/SCDType2.yaml", 'dvdrental.staff')
test = ToDV(testread.path, testread.key)
sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2006-02-16")
targetDF = utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema())
# print(targetDF)
newSourceDF = utils.toSCD2(sourceDF, targetDF)
ganaretedDF = utils.generateSurogateKey(newSourceDF,test.getCode(), test.getNaturalKey() )
print(ganaretedDF)
utils.fillPosgres(ganaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())



                        # S T A G I N G
# # initial
# test = toStaging_initial('dvdrental','public')
# tbname_Df = utils.getTbaleList(test.getSourceDBName,test.getSourceSchema)
# for tbname in tbname_Df['tablename']:
#     utils.fillPosgres(utils.getDF( tbname ,test.getSourceSchema),'DBStaging','dvdrental', tbname)


# full load
# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.inventory')
# test = toStaging_full(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
# utils.fillPosgres(sourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())


# incremental
# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/tostaging/dvdrental/incremental.yaml", 'public.customer')
# test = toStaging_incremental(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2000-02-10", "2024-02-16")
# newsourceDF = utils.addInsertionDate(sourceDF)
# # print(newsourceDF)
# utils.fillPosgres(newsourceDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName())

# clean staging
# cleantestread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.category')
# t = CleanStaging()
# t.cleanStaging('DBStaging', 'dvdrental', 'category', 2023, 12, 28)
