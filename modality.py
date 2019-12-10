'''
Created on 2 Nov 2016

@author: Administrator
'''
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import last_day,month,year
import csv, io,keyring,os,re,datetime
import time,sys
from pyspark.sql.types import *

stime = time.time()
Datelog = datetime.datetime.now().strftime('%Y-%m-%d')
start_time = datetime.datetime.now().strftime('%H:%M:%S')


try:
    conf = SparkConf().setMaster("local[8]").setAppName("Modality")\
           .set("spark.sql.shuffle.partitions",16)\
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
        .set("spark.driver.memory","30g")\
        .set("spark.executor.memory","30g")\
        .set("spark.driver.cores",8)

    sc = SparkContext(conf = conf)
    sqlctx = SQLContext(sc)
    spark = SparkSession.builder.appName("Modality").getOrCreate()

    ####################### DB Credentials  ###########################
    owmode = "overwrite"
    opmode = "append"
    csvpath=os.path.dirname(os.path.realpath(__file__))
    csvpath=csvpath[0:len(csvpath)-3]
    dbentity=csvpath[len(csvpath)-6:len(csvpath)-1]
    dbentity=dbentity.lower()
    
    
    ConfigVariables = sqlctx.read.csv(path=csvpath+"conf\ConfigVariables.csv",header="true")
    NoofRows= ConfigVariables.count()
    for i in range(0,NoofRows):
        exec(ConfigVariables.select(ConfigVariables.Variable).collect()[i]["Variable"]+"="+ConfigVariables.select(ConfigVariables.Value).collect()[i]["Value"])

    connections = sqlctx.read.csv(path=csvpath+"conf\connections.csv",header="true")
    connections.cache()
    NoofRows= connections.count()

    for i in range(0,NoofRows):
        exec(connections.select(connections.Variable).collect()[i]["Variable"]+"="+chr(34)+connections.select(connections.Value).collect()[i]["Value"]+chr(34)) 

    
    path=os.path.realpath(__file__)
    index=path.find("src")
    index=path[index-6:index-1]
    

    Sqlurl="jdbc:sqlserver://"+keyring.get_password(index,"sqlip")+":1433;\
    databaseName="+SQLDB+";\
    user="+keyring.get_password(index,"sqluser")+";\
    password="+keyring.get_password(index,"sqlpassword")

    Configurl="jdbc:sqlserver://localhost:1433;\
    databaseName="+CONFIGDB+";\
    user="+keyring.get_password("kockpit","configuser")+";\
    password="+keyring.get_password("kockpit","configpassword")

    Postgresurl = "jdbc:postgresql://"+POSTGREURL+"/"+POSTGREDB
    Postgresprop= {
        "user":keyring.get_password("kockpit","psgruser"),
        "password":keyring.get_password("kockpit","psgrpassword"),
        "driver": "org.postgresql.Driver" 
    }

    DB=csvpath[len(csvpath)-6:len(csvpath)-3]
    Company=csvpath[len(csvpath)-3:len(csvpath)-1]
    ####################### DB Credentials  ###########################

    maxrows="(SELECT "+chr(34)+"Table"+chr(34)+" AS Table,\
                 "+chr(34)+"Value"+chr(34)+" AS Value\
                FROM "+dbentity+".partitioninfo) AS partitioninfo"
    maxrows = sqlctx.read.format("jdbc").options(url=Postgresurl, dbtable=maxrows,\
              user=keyring.get_password("kockpit","psgruser"),password=keyring.get_password("kockpit","psgrpassword"),\
              driver= "org.postgresql.Driver").load() 

    NoofRows= maxrows.count()
    for i in range(0,NoofRows):
        exec(maxrows.select(maxrows.table).collect()[i]["table"]+"="+chr(34)+str(maxrows.select(maxrows.value).collect()[i]["value"])+chr(34))

    ####################### Upper Bound  ###########################

    ######################## Start Date ###########################
    '''
    query= "(SELECT StartDate FROM [Vw_getcompanyDetails]\
    where CompanyName='"+Company+"' and DBName='"+DB+"') as data1"
    df = sqlctx.read.format("jdbc").options(url=Configurl,dbtable=query, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .load()
    '''

    
    data_path = csvpath[:csvpath.find("DB")]
    table = sqlctx.read.parquet(data_path+"\data\\CompanyDetails")
    
    df = table.filter(table['DBName'] == DB ).filter(table['NewCompanyName'] == Company)
    
    Calendar_StartDate = df.select(df.StartDate).collect()[0]["StartDate"]
#    print(Calendar_StartDate)
    
    
    #Calendar_StartDate = datetime.datetime.strptime(StartDate,'%d/%m/%Y').date()
    '''if datetime.date.today().month>MonthStart-1:
        UIStartYr=datetime.date.today().year-Years+1

    else:
        UIStartYr=datetime.date.today().year-Years
    UIStartDate=datetime.date(UIStartYr,MonthStart,1)
    Calendar_StartDate = datetime.datetime.strptime(Calendar_StartDate, '%m/%d/%Y').date()    

    UIStartDate=max(Calendar_StartDate,UIStartDate)'''
    ################################ Start Date #################################
    
    
    mod1="(SELECT *\
      FROM ["+NAVSTRING+"Modality] ) AS GLEntry"        

    mod = sqlctx.read.format("jdbc").options(
    url=Sqlurl, 
    dbtable=mod1,
    driver="com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #print(mod1)
    #mod.show()
    #exit()
    mod.cache()
    mod.write.jdbc(url=Postgresurl, table=dbentity+".Modality", mode=owmode, properties=Postgresprop)
    mod.show(2,False)
    print(dbentity)
    end_time = datetime.datetime.now().strftime('%H:%M:%S')
    etime = time.time()-stime 
    #a = Row("Start_time","End_time","File_Name","DB","EN","Status","Log_Staus","Rows"=T1.count(),"Columns"=len(T1.columns)]
    try:
        IDEorBatch = sys.argv[1]
    except Exception as e :
        IDEorBatch = "IDLE"
    log_dict = [{'Date':Datelog,'Start_Time':start_time,'End_Time':end_time,'Run_Time':etime,'File_Name':'Modality','DB':DB,'EN':Company,'Status':'Completed','Log_Status':'Completed','Rows':mod.count(),'Columns':len(mod.columns),'Source':IDEorBatch}]
    #from pyspark.sql.types import StructType, StructField, StringType,IntegerType,DateType
    schema = StructType([
        StructField('Date',StringType(),True),
        StructField('Start_Time',StringType(),True),
        StructField('End_Time', StringType(),True),
        StructField('Run_Time',FloatType(),True),
         StructField('File_Name',StringType(),True),
          StructField('DB',StringType(),True),
           StructField('EN', StringType(),True),
        StructField('Status',StringType(),True),
         StructField('Log_Status',StringType(),True),
          StructField('Rows',IntegerType(),True),
          StructField('Columns',IntegerType(),True),
        StructField('Source',StringType(),True)]
    )
    log_df = spark.createDataFrame(log_dict,schema)
    log_df.write.jdbc(url=Postgresurl, table="logs.logtable", mode=opmode, properties=Postgresprop)
except Exception as ex:
    

    end_time = datetime.datetime.now().strftime('%H:%M:%S')
    etime = time.time()-stime 
    print(ex)
    exc_type,exc_value,exc_traceback=sys.exc_info()
    print("Error:",ex)
    print("type - "+str(exc_type))
    print("File - "+exc_traceback.tb_frame.f_code.co_filename)
    print("Error Line No. - "+str(exc_traceback.tb_lineno))
    try:
        IDEorBatch = sys.argv[1]
    except Exception as e :
        IDEorBatch = "IDLE"
    log_dict = [{'Date':Datelog,'Start_Time':start_time,'End_Time':end_time,'Run_Time':etime,'File_Name':'ManualCOGS','DB':DB,'EN':Company,'Status':'Failed','Log_Status':ex,'Rows':0,'Columns':0,'Source':IDEorBatch}]
    #from pyspark.sql.types import StructType, StructField, StringType,IntegerType,DateType
    schema = StructType([
        StructField('Date',StringType(),True),
        StructField('Start_Time',StringType(),True),
        StructField('End_Time', StringType(),True),
        StructField('Run_Time',FloatType(),True),
         StructField('File_Name',StringType(),True),
          StructField('DB',StringType(),True),
           StructField('EN', StringType(),True),
        StructField('Status',StringType(),True),
         StructField('Log_Status',StringType(),True),
          StructField('Rows',IntegerType(),True),
          StructField('Columns',IntegerType(),True),
        StructField('Source',StringType(),True)]
    )
    log_df = spark.createDataFrame(log_dict,schema)
    log_df.write.jdbc(url=Postgresurl, table="logs.logtable", mode=opmode, properties=Postgresprop)

