#funtion to transform date in str YYYY-MM-DD to datetime
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth,udf,col,date_format, concat_ws, concat, when, array
from pyspark.sql.types import *
import re
from pyspark.sql import functions as F



def day_to_date(day_str):
    day_date =  datetime.datetime.strptime(day_str, '%Y-%m-%d').date()
    return(day_date)

def read_table(table_location,table_schema):
    df=spark.read.option("header","false").option("delimiter","|") \
        .option("nullValue", "\\N") \
        .option("charset", "UTF8") \
        .schema(table_schema).csv(table_location)
    return(df)

def read_db_credentials(credential_file):
    db_cred = spark.read.option("multiline","true").json(credential_file)
    x=db_cred.collect()
    dbName=x[0][0]
    dbPassword=x[0][1]
    dbUser=x[0][2]
    dbHost=x[0][3]
    return dbName, dbPassword,dbUser,dbHost

def read_conf(location, name):
    df=spark.read.option("delimiter",",").option("header","true").option("dateFormat", "yyyy/MM/dd HH:mm:ss").option("inferSchema", "true") \
        .csv(location+"/"+name)
    return(df)

def createDf(table, path,dbName, dbPassword,dbUser,dbHost):
    table_location=path+table
    query = "select * from "+table+" limit 1"
    url="jdbc:postgresql://"+dbHost+"/"+dbName+"?user="+dbUser+"&password="+dbPassword
    dbtbl = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver").option("url",url) \
        .option("query", query).load()
    df = spark.read \
        .option("header","false").option("delimiter","|") \
        .option("nullValue", "\\N").option("charset", "UTF8").schema(dbtbl.schema) \
        .option("quote",'"').csv(table_location)
    return(df)

def createDf_loc(table, location,path,dbName, dbPassword,dbUser,dbHost):
    table_location=path+location
    query = "select * from "+table+" limit 1"
    url="jdbc:postgresql://"+dbHost+"/"+dbName+"?user="+dbUser+"&password="+dbPassword
    dbtbl = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver").option("url",url) \
        .option("query", query).load()
    df = spark.read \
        .option("header","false").option("delimiter","|") \
        .option("nullValue", "\\\\N").option("charset", "UTF8").schema(dbtbl.schema) \
        .option("quote",'"').csv(table_location)
    return(df)

def select_range_time(df,day_ini,day_fin):
    df_ret=df.filter(year("created_at")>=day_ini.year).filter(month("created_at")>=day_ini.month).filter(dayofmonth("created_at")>=day_ini.day)
    df_ret=df_ret.filter(year("created_at")<=day_fin.year).filter(month("created_at")<=day_fin.month).filter(dayofmonth("created_at")<=day_fin.day)
    return(df_ret)

# def select_range_time(df,day_ini,day_fin):
#     day_ini_c = datetime.datetime.combine(day_ini, datetime.time(0, 0))
#     day_fin_c = datetime.datetime.combine(day_fin, datetime.time(0, 0))
#     df_filtered=df.where((col("created_at").between(day_ini_c,day_fin_c)))
#     return(df_filtered)

# def select_field_range_time(df,field,day_ini,day_fin):
#     day_ini_c = datetime.datetime.combine(day_ini, datetime.time(0, 0))
#     day_fin_c = datetime.datetime.combine(day_fin, datetime.time(0, 0))
#     df_filtered=df.where((col(field).between(day_ini_c,day_fin_c)))
#     return(df_filtered)



def select_field_range_time(df,field,day_ini,day_fin):
    df_ret=df.filter(year(field)>=day_ini.year).filter(month(field)>=day_ini.month).filter(dayofmonth(field)>=day_ini.day)
    df_ret=df_ret.filter(year(field)<=day_fin.year).filter(month(field)<=day_fin.month).filter(dayofmonth(field)<=day_fin.day)
    return(df_ret)


def clean_null(df):
    ret=df.fillna(0)
    return(ret)

def df_write_csv(df,location,name, mode='overwrite'):
    df.repartition(1).write.format('com.databricks.spark.csv').mode(mode).option("sep",",").save(location+"csvs/"+name+".csv",header = 'true')

def df_write_es(df,domain,name, mode='overwrite'):
    df.write.format("org.elasticsearch.spark.sql").mode(mode).option("es.read.metadata", "true").option("es.nodes.wan.only","true").option("es.port","443").option("es.net.ssl","true").option("es.nodes", domain).option("es.resource", name+"/data").save()


def nulls_per_col(df):
    tnullspercol = {}
    for k in df.columns:
        nullRows = df.where(col(k).isNull()).count()
        if(nullRows>0):
            tnullspercol[k]=nullRows
    return(tnullspercol)


def show_shop(df, shop_id):
    df.filter(col("shop_id")==shop_id).show()
    return


def show_orderdesc(df, colorder):
    df.orderBy(col(colorder),ascending=False).show()
    return

def filter_shop(df, shop_id):
    return(df.filter(col("shop_id")==shop_id))


def q_nulls(df, col_name):
    return(df.filter(col(col_name).isNull()).count())


def rename_columns(df, newcolnames):
    for c,n in zip(df.columns,newcolnames):
        df=df.withColumnRenamed(c,n)
    return(df)


#function to read all the tables: period of time for raw tables - months to go back for pt pi

def read_table_range_time(table, path,dbName,dbPassword,dbUser,dbHost,field,date_ini,date_fin):
    df = createDf(table, path,dbName,dbPassword,dbUser,dbHost)
    df=select_field_range_time(df,field,date_ini,date_fin)
    return(df)

def read_table_range_time_loc(table, location, path,dbName,dbPassword,dbUser,dbHost,field,date_ini,date_fin):
    df = createDf_loc(table,location, path,dbName,dbPassword,dbUser,dbHost)
    df=select_field_range_time(df,field,date_ini,date_fin)
    return(df)

def create_pt_pi(pt,pi):
    pt_pi=pt.join(pi, pt.id==pi.purchase_transaction_id) \
        .select(pt.id,pt.business_name, pt.shop_id,pt.order_number,pi.created_at,pi.external_id,pi.scale,pi.size_numeric, \
                pi.item_source,pi.status,pi.size_id) \
        .withColumn("size_float", col("size_numeric").cast(FloatType())).withColumnRenamed('id', 'pt_id'). \
        withColumn("import_date",date_format(col("created_at").cast(DateType()),"yyyy/MM/dd 00:00:00"))
    return(pt_pi)

def read_plugin(table_location,yesterday):
    year=yesterday.year
    month=yesterday.month
    day=yesterday.day
    partition=table_location+'/year='+str(year)+'/month='+str(month)+'/day='+str(day)
    df=spark.read.option("header","true").option("delimiter","|") \
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss").option("inferSchema", "true") \
        .option("nullValue", "\\N") \
        .option("charset", "UTF8") \
        .csv(partition)
    return(df)

def read_data(path,date_ini,date_fin, plugin_loc):
    dbName,dbPassword,dbUser,dbHost=read_db_credentials(credential_file)
    ds = createDf("dashboard_shops", path,dbName,dbPassword,dbUser,dbHost)
    ds = ds.select(['shop_id','business_name'])
    pt = createDf("purchase_transactions", path,dbName,dbPassword,dbUser,dbHost)
    pt = pt.join(ds,['shop_id'])
    pi = read_table_range_time("purchase_items", path,dbName,dbPassword,dbUser,dbHost,"created_at",date_ini,date_fin)
    pi=pi.filter(col("item_source")==1)
    reco=read_table_range_time_loc("recommendations","recommendations_last_3_month", path,dbName,dbPassword,dbUser,dbHost,"created_at",date_ini,date_fin)
    reco=reco.join(ds,['shop_id']).withColumn("import_date",date_format(col("created_at").cast(DateType()),"yyyy/MM/dd 00:00:00"))
    plugin=read_plugin(plugin_loc,date_ini)
    plugin=plugin.join(ds,['shop_id']).withColumn("import_date",date_format(col("effective_date").cast(DateType()),"yyyy/MM/dd 00:00:00"))
    return(pt,pi,reco, plugin)

#calculate stats for purchases
def calc_purchase_stats(pt_pi, keycols):
    purchase_stats = pt_pi.groupBy(keycols).agg(F.sum(F.when(col('status')==0,F.lit(1)). \
                                                      otherwise(F.lit(0))).alias("cart_items"),
                                                F.sum(F.when(col('status')==1,F.lit(1)). \
                                                      otherwise(F.lit(0))).alias("pur_items"),
                                                F.sum(F.when((col('status')==1) & (col('size_id').isNull()),F.lit(1)). \
                                                      otherwise(F.lit(0))).alias("pur_items_no_size_id"), \
                                                F.sum(F.when((col('status')==1) & (col('external_id').isNull()),F.lit(1)). \
                                                      otherwise(F.lit(0))).alias("pur_items_no_external_id"))
    return(purchase_stats)


def calc_reco_stats(reco, keycols):
    reco_stats=reco.groupBy(keycols).agg(F.count(F.lit(1)).alias("reco_done"), \
                                         F.sum(F.when(col('output_size_id').isNull(), \
                                                      F.lit(1)).otherwise(F.lit(0))).alias("no_output_size_id"),
                                         F.sum(F.when(col('output_size_id').isNull(), \
                                                      F.lit(0)).otherwise(F.lit(1))).alias("with_output_size_id"))
    return(reco_stats)


def calc_plugin_stats(plugin, keycols):
    plugin_stats=plugin.groupBy(keycols).agg(F.sum(col('loadings_all')).alias("loadings_all"), \
                                             F.sum(col('openings_all')).alias("openings_all"))
    return(plugin_stats)


# def concat_shoes_ids(sps_id, ss_id, s_id):
#     return(str(sps_id)+'-'+str(ss_id)+'-'+str(s_id))
def concat_shoes_ids(sps_id, ss_id, s_id):
    a=sps_id.encode('utf-8')
    b=str(ss_id)
    c=str(s_id)
    return(a+'-'+b+'-'+c)



def merge_outputs(df1,df2,keycols):
    ret=df1.join(df2,keycols,how="outer")
    return(ret)

def exclude_no_active_shops(df,conf_file):
    df_ret=df.join(conf_file.filter(col("portfolio_active")=='n'), ["shop_id"], how='left_anti')
    return(df_ret)


def concat_arr(a):
    result = ''
    for word in a:

        result += str(word)+' '
        if(word is not None):
            result+='\n'
    return result

def concat_2arr(an, av):
    result = ''
    for i in range(len(an)):
        if(str(av[i])!='None'):
            result += an[i]+':\t'+str(av[i])+'\n'
    return result




#1. set global variables and transform string date parameters into date
import datetime
import argparse
spark = SparkSession.builder.appName("real_time_stats_collector").getOrCreate();
sc = spark.sparkContext;
sc._conf.setAll([('spark.sql.broadcastTimeout', '-1'),('spark.sql.autoBroadcastJoinThreshold','-1')])
path='s3://ssm-datalake/ssm-input/db/'
path_output='s3://ssm-datalake/ssm-output/'
path_configuration='s3://ssm-datalake/ssm-code/app_configuration'
path_emr_imports="s3://shoesizeme/uploads/imports/"
parser = argparse.ArgumentParser()
parser.add_argument("--days_before", help="how many days before today- default 1")
args = parser.parse_args()
if args.days_before:
    days_before=int(args.days_before)
else:
    days_before=1

today = datetime.date.today()

yesterday = today - datetime.timedelta(days=days_before)
day_ini_date=yesterday
day_fin_date=yesterday
es_index_sufix=yesterday.strftime("%Y-%m-%d")
esdomain="https://vpc-ssm-esdomain-prod-cshee27qvhutha656iggaxw5um.eu-central-1.es.amazonaws.com"
credential_file='s3://ssm-datalake/ssm-code/emr/database_credentials.json'
conf_filename="alert_configuration.csv"
plugin_loc='s3://ssm-datalake/ssm-output/plugin_logs/hourly.csv.partitioned/'

# esdomain="https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com"
# #to process a complete month
# day_ini_str="2020-04-01"
# day_fin_str="2020-04-30"
# es_index_sufix="2020-04"
# day_ini_date=day_to_date(day_ini_str)
# day_fin_date=day_to_date(day_fin_str)


#2. read data from s3 into dataframes
pt,pi,reco, plugin=read_data(path, day_ini_date, day_fin_date, plugin_loc)
#3. generate dataframe pt_pi, joining pt and pi.
pt_pi=create_pt_pi(pt,pi)
# pt_pi.printSchema()


#4. generate purchase_api_stats including how many items received by shop in the cart and purchases. The stats includes how many purchases don't have size or external_id
keycols=['shop_id','business_name','import_date']
purchase_api_stats=calc_purchase_stats(pt_pi, keycols)
# purchase_api_stats.orderBy(col('shop_id')).show()

#5. generate reco_stats including how many reco received by shop, and how many don't have output_size_if
reco_stats=calc_reco_stats(reco, keycols)
# reco_stats.orderBy(col('shop_id')).show()

#6. generate plugin_stats including how many plugin loadings and opening by shop
plugin_stats=calc_plugin_stats(plugin, keycols)


# merge stats
# 7. join results in real_time_stats dataframe. transform null values in 0
real_time_stats=merge_outputs(purchase_api_stats,reco_stats,keycols)
real_time_stats=merge_outputs(real_time_stats,plugin_stats,keycols)
real_time_stats=clean_null(real_time_stats)
# real_time_stats.show()


#8. Save in S3 and ES
df_write_es(real_time_stats,esdomain,"real_time_stats-"+es_index_sufix)
df_write_csv(real_time_stats, path_output,"real_time_stats",mode="append")




