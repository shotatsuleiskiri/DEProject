import myFramework.utils.utils as utils
from sqltostaging.initial.toStaging import ToStaging as toStaging_initial
from sqltostaging.full.toStaging import ToStaging as toStaging_full
from sqltostaging.incremental.toStaging import ToStaging as toStaging_incremental
from managment.cleanstaging.cleanStaging import CleanStaging
from myFramework.utils.readYaml import ReadYaml
# from stagingtodv.SCDType1.toDV import ToDV
from stagingtodv.SCDType2.toDV import ToDV

# ---------------------------DV----------------

#link ,
# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/toDV/dvdrental/link.yaml", 'dvdrental.film_category')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(), test.getfilterColumn(),  "2005-05-26", "2020-05-26").drop("insertion_date",  axis=1)
# dest_col_list = list(utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns)
# genaretedDF = utils.generateSurogateKey(sourceDF,test.getCode(), list(test.getSurogateKey().split(" ")),dest_col_list)
# # print(genaretedDF)
# utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(),test.getInsertionType())



# Full - SCDType1
# create ReadYaml object

# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/toDV/dvdrental/SCDType1.yaml", 'dvdrental.category')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema())
# dest_col_list = list(utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns)
# generatenaturalkey = utils.GenerateNaturalKey(sourceDF, test.getNaturalKey())
# ganaretedDF = utils.generateSurogateKey(generatenaturalkey,test.getCode(), list(test.getSurogateKey().split(" ")), dest_col_list)
# utils.fillPosgres(ganaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(), test.getInsertionType())


# incremental
# create ReadYaml object
#
# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/toDV/dvdrental/incremental.yaml", 'dvdrental.rental')
# test = ToDV(testread.path, testread.key)
# sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2007-01-01", "2024-02-15").drop("insertion_date",  axis=1)
# dest_col_list = utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns
# generatenaturalkey = utils.GenerateNaturalKey(sourceDF, test.getNaturalKey())
# genaretedDF = utils.generateSurogateKey(generatenaturalkey,test.getCode(), list(test.getSurogateKey().split(" ")), list(dest_col_list))
# # print(genaretedDF.columns)
# utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(),test.getInsertionType())
#

# SCDType2
testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/toDV/dvdrental/SCDType2.yaml", 'dvdrental.store')
test = ToDV(testread.path, testread.key)
sourceDF = utils.getDF(test.getSourceDBName(), test.getTSourceTableName(),test.getSourceSchema(),test.getfilterColumn(), "2003-05-26", "2014-02-28").drop("last_update",  axis=1)
targetDF = utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema())
SCD_DF = utils.scdtest(sourceDF,targetDF, list(test.getSurogateKey().split(" ")), test.getNaturalKey())
dest_col_list = list(utils.getDF(test.getDestDBName(), test.getDestTbaleName(),test.getDestSchema()).columns)
genaretedDF = utils.generateSurogateKey(SCD_DF, test.getCode(), list(test.getSurogateKey().split(" ")),dest_col_list)
# print(genaretedDF)
utils.fillPosgres(genaretedDF,f'{test.getDestDBName()}',f'{test.getDestSchema()}',test.getDestTbaleName(), test.getInsertionType())

# ,test.getfilterColumn(), "2013-05-26", "2005-05-26"



                        # S T A G I N G
# # initial
# test = toStaging_initial('dvdrental','public')
# tbname_Df = utils.getTbaleList(test.getSourceDBName,test.getSourceSchema)
# for tbname in tbname_Df['tablename']:
#     utils.fillPosgres(utils.getDF( tbname ,test.getSourceSchema),'DBStaging','dvdrental', tbname)


# full load
# testread = ReadYaml("/Users/mariammakharadze/PycharmProjects/DEProject/conf/tostaging/dvdrental/full.yaml", 'public.store')
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
