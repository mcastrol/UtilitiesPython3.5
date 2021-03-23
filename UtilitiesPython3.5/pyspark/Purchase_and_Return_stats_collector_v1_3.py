#funtion to transform date in str YYYY-MM-DD to datetime
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth,udf,col,date_format
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


def read_table_old(table_location,table_schema):
    df=spark.read.option("delimiter","|").schema(table_schema).csv(table_location)
    return(df)


#filter from df only rows created on day to process

def select_range_time(df,day_ini,day_fin):
    df_ret=df.filter(year("created_at")>=day_ini.year).filter(month("created_at")>=day_ini.month).filter(dayofmonth("created_at")>=day_ini.day)
    df_ret=df_ret.filter(year("created_at")<=day_fin.year).filter(month("created_at")<=day_fin.month).filter(dayofmonth("created_at")<=day_fin.day)
    return(df_ret)

def register_df(namedf,df):
    sqlContext.sql("drop table if exists "+namedf)
    sqlContext.registerDataFrameAsTable(df,namedf)

def create_pt_pi(pt,pi):
    pt_pi=pt.join(pi, pt.id==pi.purchase_transaction_id) \
        .select(pt.id,col("shop_id"),pt.order_number,pi.external_id,pi.scale,pi.size_numeric,pi.item_source,pi.status) \
        .withColumn("size_float", col("size_numeric").cast(FloatType())).withColumnRenamed('id', 'pt_id')
    return(pt_pi)


#reading raw import purchases (rip dataframe)

def read_rip(path, date_ini,date_fin):
    table_schema = StructType([StructField('id', IntegerType(), True),
                               StructField('external_id',StringType(), False),
                               StructField('order_number',StringType(), False),
                               StructField('order_time',TimestampType(), False),
                               StructField('size',StringType(), False),
                               StructField('size_scale',StringType(), False),
                               StructField('shop_id', StringType(), False),
                               StructField('status', IntegerType(), True),
                               StructField('import_id', StringType(), False),
                               StructField('created_at',TimestampType(), False),
                               StructField('updated_at', TimestampType(), False),
                               StructField('import_notice', IntegerType(), True)])

    table_name='raw_import_purchases'
    table_location=path+table_name+'_col/'
    rip=read_table(table_location,table_schema)
    rip=rip.dropna(subset=["shop_id","external_id","size","created_at"])
    rip=select_range_time(rip,date_ini,date_fin)
    return(rip)


#reading raw import returns (rir dataframe)
def read_rir(path, date_ini,date_fin):
    table_schema = StructType([StructField('id', IntegerType(), False),
                               StructField('external_id',StringType(), True),
                               StructField('order_number',StringType(), True),
                               StructField('return_time',TimestampType(), True),
                               StructField('size',StringType(), True),
                               StructField('size_scale',StringType(), True),
                               StructField('quantity', IntegerType(), True),
                               StructField('shop_id', StringType(), True),
                               StructField('status', IntegerType(), True),
                               StructField('import_id', StringType(), True),
                               StructField('created_at', TimestampType(), True),
                               StructField('updated_at', TimestampType(), True),
                               #                             StructField('reason',StringType(), True),
                               StructField('import_notice',IntegerType(), True)
                               ])
    table_name='raw_import_returns'
    table_location=path+table_name+'_col/'
    rir=read_table(table_location,table_schema)
    rir=rir.dropna(subset=["shop_id","external_id","size","created_at"])
    rir=select_range_time(rir,date_ini,date_fin)
    return(rir)




#reading raw import returns (rir dataframe)
def read_imports(path):
    table_schema = StructType([StructField('id', IntegerType(), False),
                               StructField('created_at', TimestampType(), True),
                               StructField('updated_at', TimestampType(), True),
                               StructField('shop_id', StringType(), True),
                               StructField('import_type', IntegerType(), True),
                               StructField('import_subject', IntegerType(), True)
                               ])
    table_name='imports'
    table_location=path+table_name+'_col/'
    imports=read_table(table_location,table_schema)
    return(imports)




#reading purchase_transactions
def read_pt(path):
    table_schema = StructType([StructField('id', IntegerType(), True),
                               StructField('shop_id', IntegerType(), True),
                               StructField('order_number',StringType(), True),
                               StructField('order_time',TimestampType(), True),
                               StructField('email',StringType(), True),
                               StructField('ip',StringType(), True),
                               StructField('created_at', TimestampType(), True),
                               StructField('updated_at', TimestampType(), True),
                               StructField('action',IntegerType(), True),
                               StructField('session_token',StringType(), True),
                               StructField('sid',IntegerType(), True),
                               StructField('session_id',IntegerType(), True),
                               StructField('profile_id',IntegerType(), True),
                               StructField('shoe_shelves_id',IntegerType(), True)])
    table_name='purchase_transactions'
    table_location=path+table_name
    pt=read_table(table_location,table_schema)
    pt = pt.dropna(subset=["id","shop_id","order_number","created_at"])
    return(pt)

#reading purchase_items
def read_pi(path):
    table_schema = StructType([StructField('id', IntegerType(), True),
                               StructField('purchase_transaction_id', IntegerType(), True),
                               StructField('scale',StringType(), True),
                               StructField('size_id', IntegerType(), True),
                               StructField('item_name',StringType(), True),
                               StructField('created_at', TimestampType(), True),
                               StructField('updated_at', TimestampType(), True),
                               StructField('model',StringType(), True),
                               StructField('size_numeric',StringType(), True),
                               StructField('brand',StringType(), True),
                               StructField('external_id',StringType(), True),
                               StructField('currency',StringType(), True),
                               StructField('price',FloatType(), True),
                               StructField('quantity',IntegerType(), True),
                               StructField('status',IntegerType(), True),
                               StructField('item_source',IntegerType(), True),
                               StructField('return_reason',IntegerType(), True),
                               StructField('shop_shoe_id',IntegerType(), True),
                               StructField('size_width',StringType(), True)])
    table_name='purchase_items'
    table_location=path+table_name
    pi=read_table(table_location,table_schema)
    pi = pi.dropna(subset=["purchase_transaction_id","size_numeric","external_id","item_source"])
    return(pi)


#reading dashboard shop
def read_ds(path, date_ini,date_fin):
    table_schema = StructType([StructField('id', IntegerType(), True),
                               StructField('shop_id',  StringType(), False),
                               StructField('business_name', StringType(), False),
                               StructField('created_at',TimestampType(), False),
                               StructField('updated_at', TimestampType(), False),
                               StructField('vat', FloatType(), True)])

    table_name='dashboard_shops'
    table_location=path+table_name
    ds=read_table(table_location,table_schema)
    return(ds)

#auxiliar functions for get size
def find_first_int(numstr):
    num=""
    numarr=re.findall('\d+', numstr)
    if len(numarr)>0:
        num=numarr[0]
    return(num)

def find_first_float(numstr):
    num=""
    numarr=re.findall('\d+(?:\.\d+)?', numstr)
    if len(numarr)>0:
        num=numarr[0]
    return(num)
#function to add a transformed column size_format in rip df
def transfo_size(size_p):
    if(size_p is not None):
        size=size_p.replace(",",".")
        new_size=size
        if("1/3" in size):
            new_size=find_first_int(size)+".33"
        elif("2/3" in size):
            new_size=find_first_int(size)+".66"
        elif("1/2" in size):
            new_size=find_first_int(size)+".5"
        else:
            new_size=find_first_float(size)
    else:
        new_size=""
    return(new_size)

#function to read all the tables: period of time for raw tables - months to go back for pt pi
def read_data(path,date_ini,date_fin):
    rip=read_rip(path, date_ini,date_fin)
    rir=read_rir(path,date_ini,date_fin)
    ds=read_ds(path,date_ini, date_fin)
    ds=ds.select(col("shop_id"),col("business_name"))
    pt=read_pt(path)
    pi=read_pi(path)
    imports=read_imports(path)
    imports=imports.withColumnRenamed("id","import_id")
    rip=rip.alias('rip').join(imports,["shop_id","import_id"]).select('rip.*', col("import_type"))
    rir=rir.alias('rir').join(imports,["shop_id","import_id"]).select('rir.*', col("import_type"))
    return(rip,rir,pt,pi,ds)



#add transformed size_format in rip df that allows compares with pi.size_numeric
def rip_transfo(rip):
    transfo_size_udf = udf(transfo_size, StringType())
    rip=rip.withColumn("size_numeric", transfo_size_udf(rip["size"]))
    rip=rip.withColumn("import_date",date_format(col("created_at").cast(DateType()),"yyyy/MM/dd 00:00:00")) \
        .withColumn("size_float", col("size_numeric").cast(FloatType())) \
        .drop(col("id")).drop(col("order_time"))
    return(rip)

def rir_transfo(rir):
    transfo_size_udf = udf(transfo_size, StringType())
    rir=rir.withColumn("size_numeric", transfo_size_udf(rir["size"]))
    rir=rir.withColumn("import_date",date_format(col("created_at").cast(DateType()),"yyyy/MM/dd 00:00:00")) \
        .withColumn("size_float", col("size_numeric").cast(FloatType())) \
        .drop(col("id")).drop(col("return_time"))
    return(rir)



#calculate stats for purchases
def calc_purchase_stats(pt_pi,rip, ds):
    pt_pi=pt_pi.fillna(0)
    rip=rip.fillna(0)
    pur_base_stat=rip.join(pt_pi,["shop_id","order_number","external_id","size_float"],how="left")
    pur_stat1=pur_base_stat.groupBy("shop_id","import_date","import_id","import_type").agg(F.max("created_at").alias("created_at"),F.count(F.lit(1)).alias("raw_purchased_items")).withColumn("timestamp", date_format(col("created_at"), "yyyy/MM/dd HH:mm:ss"))
    pur_stat2=pur_base_stat.filter((col("pt_id").isNotNull()) & ((col("item_source")==0) | (col("item_source")==2))) \
        .groupBy("shop_id","import_date","import_id","import_type").count().withColumnRenamed('count', 'purchased_items')
    pur_stat3=pur_base_stat.filter((col("pt_id").isNotNull()) & (col("item_source")==2)) \
        .groupBy("shop_id","import_date","import_id","import_type").count().withColumnRenamed('count', 'confirmed_items')
    purchase_stats=pur_stat1.join(pur_stat2,["shop_id","import_date","import_id","import_type"],how="outer")
    purchase_stats=purchase_stats.join(pur_stat3,["shop_id","import_date","import_id","import_type"],how="outer")
    purchase_stats=purchase_stats.join(ds,["shop_id"])
    return(pur_base_stat,purchase_stats)


#calculate stats for returns
def calc_return_stats(pt_pi,rir, ds):
    pt_pi=pt_pi.fillna(0)
    rir=rir.fillna(0)
    ret_base_stat=rir.join(pt_pi,["shop_id","order_number","external_id","size_float"],how="left")
    ret_stat1=ret_base_stat.groupBy("shop_id","import_date","import_id","import_type").agg(F.max("created_at").alias("created_at"), \
                                                                                           F.count(F.lit(1)).alias("received_items")).withColumn("timestamp", date_format(col("created_at"), "yyyy/MM/dd HH:mm:ss"))

    ret_stat2=ret_base_stat.filter((col("pt_id").isNotNull()) & ((col("item_source")==0) | (col("item_source")==2))) \
        .groupBy("shop_id","import_date","import_id","import_type").count().withColumnRenamed('count', 'imported_items')

    return_stats=ret_stat1.join(ret_stat2,["shop_id","import_date","import_id","import_type"],how="outer")

    return_stats=return_stats.join(ds,["shop_id"])
    return(ret_base_stat,return_stats)



def df_write_parquet(df, location, name):
    df.write.mode('overwrite').parquet(location+name)

def df_write_csv(df,location,name, mode='overwrite'):
    df.repartition(1).write.format('com.databricks.spark.csv').mode(mode).option("sep","|").save(location+"csvs/"+name+".csv",header = 'true')

def df_write_es(df,domain,name):
    df.write.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option("es.nodes.wan.only","true").option("es.port","443").option("es.net.ssl","true").option("es.nodes", domain).option("es.resource", name+"/data").save()

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

#1 - set variables
import datetime
import argparse
spark = SparkSession.builder.appName("purchase_stats_collector").getOrCreate();
sc = spark.sparkContext;
sc._conf.setAll([('spark.sql.broadcastTimeout', '-1'),('spark.sql.autoBroadcastJoinThreshold','-1')])
path='s3://ssm-datalake/ssm-input/db/'
path_output='s3://ssm-datalake/ssm-output/'
parser = argparse.ArgumentParser()
parser.add_argument("--days_before", help="how many days before today- default 1")
args = parser.parse_args()
if args.days_before:
    days_before=int(args.days_before)
else:
    days_before=1

today = datetime.date.today()
# days_before=1
yesterday = today - datetime.timedelta(days=days_before)
day_ini_date=yesterday
day_fin_date=yesterday
es_index_sufix=yesterday.strftime("%Y-%m-%d")
esdomain="https://vpc-ssm-esdomain-prod-cshee27qvhutha656iggaxw5um.eu-central-1.es.amazonaws.com"
# esdomain="https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com"
# #to process a complete month
# day_ini_str="2020-04-01"
# day_fin_str="2020-04-30"
# es_index_sufix="2020-04"
# day_ini_date=day_to_date(day_ini_str)
# day_fin_date=day_to_date(day_fin_str)


#2 read data
rip, rir, pt, pi,ds=read_data(path, day_ini_date, day_fin_date)


#3 transfo_rip: add size_format+ year+month+day
rip=rip_transfo(rip)

#4 transfo_rir: idem rip: add size_format+ year+month+day
rir=rir_transfo(rir)
# df_write_csv(rir, path,"rirtransfo")



#5 Calculate Purchases Stats
pt_pi=create_pt_pi(pt,pi)
pur_base_stat,purchase_stats = calc_purchase_stats(pt_pi, rip,ds)
# purchase_stats.orderBy(col("raw_purchased_items"), ascending=False).show()


#6 Calculate Return Stats
ret_base_stat,return_stats = calc_return_stats(pt_pi,rir,ds)
# return_stats.orderBy(col("received_items"), ascending=False).show()



#7 join not clear
#8 save
df_write_es(purchase_stats,esdomain,"purchase_stats-"+es_index_sufix)
df_write_es(return_stats,esdomain,"return_stats-"+es_index_sufix)
df_write_csv(purchase_stats, path_output,"purchase_stats",mode="append")
df_write_csv(return_stats, path_output,"return_stats",mode="append")



