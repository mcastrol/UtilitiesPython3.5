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
        .option("nullValue", "\\\\N").option("charset", "UTF8").schema(dbtbl.schema) \
        .option("quote",'"').csv(table_location)
    return(df)

def select_range_time(df,day_ini,day_fin):
    df_ret=df.filter(year("created_at")>=day_ini.year).filter(month("created_at")>=day_ini.month).filter(dayofmonth("created_at")>=day_ini.day)
    df_ret=df_ret.filter(year("created_at")<=day_fin.year).filter(month("created_at")<=day_fin.month).filter(dayofmonth("created_at")<=day_fin.day)
    return(df_ret)


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


def read_data(path,date_ini,date_fin):
    dbName,dbPassword,dbUser,dbHost=read_db_credentials(credential_file)
    ds = createDf("dashboard_shops", path,dbName,dbPassword,dbUser,dbHost)
    ds = ds.select(['shop_id','business_name'])
    imp = read_table_range_time("imports", path,dbName,dbPassword,dbUser,dbHost,"created_at",date_ini,date_fin)
    imp=imp.filter(col("import_subject")==0)
    imp=imp.withColumn("import_date",date_format(col("updated_at").cast(DateType()),"yyyy/MM/dd 00:00:00"))
    imp = imp.join(ds,['shop_id'])
    sps = read_table_range_time("shop_portfolio_shoes", path,dbName,dbPassword,dbUser,dbHost,"updated_at",date_ini,date_fin)
    sps=sps.withColumn("import_date",date_format(col("updated_at").cast(DateType()),"yyyy/MM/dd 00:00:00"))
    sps = sps.join(ds,['shop_id'])
    ss = createDf("shop_shoes", path,dbName,dbPassword,dbUser,dbHost)
    #     ss = ss_all.filter(col('trash')=='false')
    shoes = createDf("shoes", path,dbName,dbPassword,dbUser,dbHost)
    #     shoes = shoes_all.filter(col('trash')=='false')
    srs = createDf("size_run_sizes", path,dbName,dbPassword,dbUser,dbHost)
    si = createDf("shoe_images", path,dbName,dbPassword,dbUser,dbHost)
    return(imp,sps,ss,shoes,srs,si)

def get_stats_from_import(path_emr_imports,imp_id):
    try:
        stats_emr_name_file=path_emr_imports+str(imp_id)+"/output/1/statistics.json"
        stats_emr = spark.read.option("multiline","true").json(stats_emr_name_file)
        stats_emr = stats_emr.withColumn("id", F.lit(imp_id))
    except IOError:
        stats_emr = None
    return(stats_emr)

def gen_imp_emr(imp):
    impcol=['id','shop_id','business_name','status','import_date']
    imp_emr=imp.select(impcol).filter(col('status')>5)
    return(imp_emr)

def gen_df_emr_stats(path_emr_imports,imp):
    imp_ids=imp.select("id").rdd.flatMap(lambda x: x).collect()
    count=0
    for i in imp_ids:
        stats=get_stats_from_import(path_emr_imports,i)
        if(stats is not None):
            if(count==0):
                stats_tot=stats
            else:
                stats_tot=stats_tot.union(stats)
            count = count + 1
    return(stats_tot)


def gen_emr_stats_all(path_emr_imports, imp):
    emr_stats=gen_df_emr_stats(path_emr_imports,imp)
    cols_to_get=['id','import_date','shop_id',"business_name","gtin_count","totalRows","totalShoes","withError", \
                 "excludedByRules","excludedByFilter","withUnsetKeywords","withEmptyBrandOrArticleNumber","newShopPortfolioShoes"]
    emr_stats_1=imp.alias("imp").join(emr_stats,["id"]).select(cols_to_get)
    emr_stats_all=emr_stats_1.groupBy("import_date","shop_id", "business_name").agg(F.min(col("id")).alias("import_id"), \
                                                                                    F.count(F.lit(1)).cast(IntegerType()).alias('imports_count'), \
                                                                                    F.sum(col('totalRows')).cast(IntegerType()).alias('totalRows'), \
                                                                                    F.sum(col('totalShoes')).cast(IntegerType()).alias('totalShoes'), \
                                                                                    F.sum(col('withError')).cast(IntegerType()).alias('withError'), \
                                                                                    F.sum(col('excludedByRules')).cast(IntegerType()).alias('excludedByRules'), \
                                                                                    F.sum(col('excludedByFilter')).cast(IntegerType()).alias('excludedByFilter'), \
                                                                                    F.sum(col('withUnsetKeywords')).cast(IntegerType()).alias('withUnsetKeywords'), \
                                                                                    F.sum(col('withEmptyBrandOrArticleNumber')).cast(IntegerType()).alias('withEmptyBrandOrArticleNumber'), \
                                                                                    F.sum(col('newShopPortfolioShoes')).cast(IntegerType()).alias('newShopPortfolioShoes'), \
                                                                                    F.sum(col('gtin_count')).cast(IntegerType()).alias('gtin_count'))
    return(emr_stats_all)


def fun_count_shoes(sps,shop_shoes,shoes,srs,col_sps, col_ss, col_ss_trashed,col_shoes,col_shoes_trashed,col_shoes_trashed_noimage,col_shoes_trashed_kids):
    sps = sps.alias("sps")
    ss = shop_shoes.alias("ss")
    s = shoes.alias("s")

    count_sps=sps.groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_sps)
    cond_sps_ss = [ss.shop_portfolio_shoe_id==sps.id,ss.shop_id==sps.shop_id]

    count_ss=sps.join(ss,cond_sps_ss).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_ss)

    ss = shop_shoes.filter(col("trash")=='true').alias("ss")
    count_ss_trashed=sps.join(ss,cond_sps_ss).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_ss_trashed)

    ss = shop_shoes.alias("ss")
    count_shoes=sps.join(ss,cond_sps_ss).join(s,ss.shoe_id==s.id).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_shoes)

    s = shoes.filter(col("trash")=='true').alias("s")
    count_shoes_trashed=sps.join(ss,cond_sps_ss).join(s,ss.shoe_id==s.id).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_shoes_trashed)

    s = shoes.filter(col("trash")=='true').filter(col('main_image').isNull()).alias("s")
    count_shoes_trashed_noimage=sps.join(ss,cond_sps_ss).join(s,ss.shoe_id==s.id).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_shoes_trashed_noimage)



    srs_kids=srs.filter(col('scale')==0).filter(col('size_numeric')<=34)
    sps_kids_ids=srs_kids.select(col('shop_portfolio_shoe_id')).distinct()
    cond_sps_srs = [sps_kids_ids.shop_portfolio_shoe_id==sps.id]
    sps_kids=sps.join(sps_kids_ids, cond_sps_srs)
    s = shoes.filter(col("trash")=='true').alias("s")
    sps = sps_kids.alias("sps")
    count_shoes_trashed_kids=sps.join(ss,cond_sps_ss).join(s,ss.shoe_id==s.id).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_shoes_trashed_kids)


    count_shoes_all = count_sps.join(count_ss,["import_date","shop_id","business_name"],how='outer'). \
        join(count_ss_trashed,["import_date","shop_id","business_name"],how='outer'). \
        join(count_shoes,["import_date","shop_id","business_name"],how='outer'). \
        join(count_shoes_trashed,["import_date","shop_id","business_name"],how='outer'). \
        join(count_shoes_trashed_noimage,["import_date","shop_id","business_name"],how='outer'). \
        join(count_shoes_trashed_kids,["import_date","shop_id","business_name"],how='outer')

    return(count_shoes_all)


def fun_count_shoes_old(sps,ss,shoes,col_sps, col_ss, col_shoes):
    sps = sps.alias("sps")
    ss = ss.alias("ss")
    s = shoes.alias("s")
    count_sps=sps.groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_sps)
    cond_sps_ss = [ss.shop_portfolio_shoe_id==sps.id,ss.shop_id==sps.shop_id]

    count_ss=sps.join(ss,cond_sps_ss).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_ss)

    count_shoes=sps.join(ss,cond_sps_ss).join(s,ss.shoe_id==s.id).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_shoes)

    count_shoes_all = count_sps.join(count_ss,["import_date","shop_id","business_name"],how='outer'). \
        join(count_shoes,["import_date","shop_id","business_name"],how='outer')
    return(count_shoes_all)


def fun_count_images(sps, si, col_images):
    sps = sps.alias("sps")
    si = si.alias("si")
    cond_sps_si = [si.shop_portfolio_shoe_id==sps.id]
    count_si=sps.join(si,cond_sps_si).groupBy("import_date","sps.shop_id","sps.business_name").count().withColumnRenamed("count",col_images)
    return(count_si)






# def concat_shoes_ids(sps_id, ss_id, s_id):
#     return(str(sps_id)+'-'+str(ss_id)+'-'+str(s_id))
def concat_shoes_ids(sps_id, ss_id, s_id):
    a=sps_id.encode('utf-8')
    b=str(ss_id)
    c=str(s_id)
    return(a+'-'+b+'-'+c)


# def concat_shoes_ids(sps_id, ss_id, s_id):
#     return((sps_id.encode('utf-8')+'-'+ss_id.encode('utf-8')+'-'+s_id.encode('utf-8')))

def fun_no_srs(sps, col_no_srs):
    sps = sps.alias("sps")
    cond_sps_ss = [ss.shop_portfolio_shoe_id==sps.id,ss.shop_id==sps.shop_id]
    col_to_select = ['sps.id','import_date','sps.shop_id','sps.business_name','sps.external_id']
    #     sps_extended=sps.join(ss.withColumnRenamed('id','ss_id'),cond_sps_ss,how='left').join(s,ss.shoe_id==s.s_id,how="left").select(col_to_select)
    #     sps_extended = sps_extended.withColumn("extid_shopshoeid_shoeId",concat_shoes_ids_udf(col('external_id'), col('ss_id'), col('s_id')))
    cond_sps_srs = [srs.shop_portfolio_shoe_id==sps.id]
    col_no_srs="count_no_srs"
    count_no_srs=sps.alias('sps').join(srs,cond_sps_srs,how='left_anti').groupBy("import_date","sps.shop_id","sps.business_name"). \
        agg(F.collect_list(col('sps.external_id')).alias('list_external_ids_arr'),F.count(F.lit(1)).alias(col_no_srs)). \
        withColumn("list_external_ids", concat_ws(",",col("list_external_ids_arr"))).drop(col("list_external_ids_arr"))
    return(count_no_srs)



def sps_expected(totalShoes,withError,excludeByRules,withUnsetKeywords,withEmptyBrandOrArticleNumber,newShopPortfolioShoes):
    #     return(totalShoes-withError-excludeByRules-withUnsetKeywords-withEmptyBrandOrArticleNumber+newShopPortfolioShoes)
    return(totalShoes-withError-excludeByRules-withUnsetKeywords-withEmptyBrandOrArticleNumber)

def add_sps_expected(df):
    sps_expected_udf = udf(sps_expected, IntegerType())
    df_new = df.withColumn("sps_expected", sps_expected_udf(col("totalShoes"), col("withError"), \
                                                            col("excludedByRules"), col('withUnsetKeywords'),col('withEmptyBrandOrArticleNumber'), col('newShopPortfolioShoes')))
    return(df_new)

def alert_sps_errors_in_rules(df, keycols, alert_name):
    sps_errors_in_rules=df.filter(col("withError") > 0).groupBy(keycols).agg(F.sum(col('withError')).alias(alert_name))
    return(sps_errors_in_rules)

def alert_no_threshold(df,keycols, col_dest,str_format,col_origen,col_threshold):
    col_to_select=keycols+ [col_dest]
    dfret=df.filter(col(col_origen)< col(col_threshold)).withColumn(col_dest,F.when(col(col_origen)< col(col_threshold),
                                                                                    F.format_string(str_format, col(col_threshold).cast(IntegerType())-col(col_origen).cast(IntegerType()), \
                                                                                                    col(col_origen).cast(IntegerType()),col(col_threshold).cast(IntegerType()))).otherwise(None))
    dfret=dfret.select(col_to_select)
    return(dfret)

def alert_sps_withUnsetKeywords(df, keycols, alert_name):
    alert_sps_withUnsetKeywords_df=df.filter(col("withUnsetKeywords") > 0).groupBy(keycols).agg(F.sum(col('withUnsetKeywords')).alias(alert_name))
    return(alert_sps_withUnsetKeywords_df)


def alert_sps_withEmptyBrandOrArticleNumber(df, keycols, alert_name):
    alert_sps_withEmptyBrandOrArticleNumber_df=df.filter(col("withEmptyBrandOrArticleNumber") > 0).groupBy(keycols).agg(F.sum(col('withEmptyBrandOrArticleNumber')).alias(alert_name))
    return(alert_sps_withEmptyBrandOrArticleNumber_df)


def alert_value_no_zero(df, keycols, field_name, alert_name):
    alert_value_no_zero_df=df.filter(col(field_name) > 0).groupBy(keycols).agg(F.sum(col(field_name)).alias(alert_name))
    return(alert_value_no_zero_df)


def alert_imp_not_finished(imp, imp_emr,alert_name):
    imp_not_finished=imp.join(imp_emr,['shop_id','business_name','import_date'],how='left_anti')
    imp_not_finished=imp_not_finished.groupBy(['shop_id','business_name','import_date','id']).agg(F.count(F.lit(1)).alias(alert_name)). \
        withColumnRenamed('id','import_id')
    return(imp_not_finished)


def alert_trashed_shoes(df,keycols, col_dest,str_format,col_origen,col_excep_1, col_excep_2):
    col_to_select=keycols+ [col_dest]
    df = df.withColumn('shoes_trashed_to_check', col(col_origen)-col(col_excep_1)-col(col_excep_2))
    dfret=df.filter(col('shoes_trashed_to_check') > 0).withColumn(col_dest,F.when(col('shoes_trashed_to_check') > 0, \
                                                                                  F.format_string(str_format,col('shoes_trashed_to_check'), col(col_origen).cast(IntegerType()), \
                                                                                                  col(col_excep_1).cast(IntegerType()),col(col_excep_2).cast(IntegerType()))).otherwise(None))
    dfret=dfret.select(col_to_select)
    return(dfret)

def alert_less_ss_than_sps(df,keycols, col_dest,str_format,col_origen,col_trashed, col_threshold):
    col_to_select=keycols+ [col_dest]
    df = df.withColumn('ss_no_trashed',col(col_origen)-col(col_trashed))
    dfret=df.filter(col('ss_no_trashed')< col(col_threshold)).withColumn(col_dest,F.when(col('ss_no_trashed')< col(col_threshold), \
                                                                                         F.format_string(str_format, col(col_threshold).cast(IntegerType())-col('ss_no_trashed').cast(IntegerType()), \
                                                                                                         col(col_origen).cast(IntegerType()),col(col_trashed).cast(IntegerType()), \
                                                                                                         col(col_threshold).cast(IntegerType()))).otherwise(None))
    dfret=dfret.select(col_to_select)
    return(dfret)

def alert_less_sps_than_ss(df,keycols, col_dest,str_format,col_sps, col_ss,col_ss_trashed):
    col_to_select=keycols+ [col_dest]
    df = df.withColumn('ss_no_trashed', col(col_ss)-col(col_ss_trashed))
    dfret=df.filter(col(col_sps)< col('ss_no_trashed')).withColumn(col_dest,F.when(col(col_sps)< col('ss_no_trashed'), \
                                                                                   F.format_string(str_format, col('ss_no_trashed').cast(IntegerType())-col(col_sps).cast(IntegerType()), \
                                                                                                   col(col_sps).cast(IntegerType()),col(col_ss).cast(IntegerType()), \
                                                                                                   col(col_ss_trashed).cast(IntegerType()))).otherwise(None))
    dfret=dfret.select(col_to_select)
    return(dfret)

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


# def ss_with_no_shoes(sps,ss,shoes,shoes_all):
#     sps = sps.alias("sps")
#     ss = ss.alias("ss")
#     s = shoes.alias("s")
#     cond_sps_ss = [ss.shop_portfolio_shoe_id==sps.id,ss.shop_id==sps.shop_id]
#     ss_no_shoes=sps.join(ss,cond_sps_ss).join(s,ss.shoe_id==s.id,how='left_anti').select('ss.*'). \
#     withColumnRenamed('id','ss_id').withColumnRenamed('created_at','ss_created_at'). \
#     withColumnRenamed('updated_at','ss_updated_at').withColumnRenamed('trash','ss_trash'). \
#     withColumnRenamed('targeted_count','ss_targeted_count')
#     ss_no_shoes=shoes_all.join(ss_no_shoes,ss.shoe_id==s.id)
#     return(ss_no_shoes)

#generates a df with  sps 14 first columns+ss columns(if exists) of sps cases with no related ss
def sps_with_no_ss(sps,shop_shoes):
    sps = sps.alias("sps")
    ss = shop_shoes.filter(col("trash")=='false').alias("ss")
    cond_sps_ss = [ss.shop_portfolio_shoe_id==sps.id,ss.shop_id==sps.shop_id]
    sps_no_ss=sps.join(ss,cond_sps_ss,'left_anti'). \
        withColumnRenamed('id','sps_id').withColumnRenamed('shop_id','sps_shop_id').drop('business_name').drop('import_date'). \
        withColumnRenamed('updated_at','sps_updated_at').withColumnRenamed('created_at','sps_created_at')
    sps_no_ss=sps_no_ss.select(sps_no_ss.columns[:14])
    ss = shop_shoes.alias("ss")
    cond_sps_no_ss = [sps_no_ss.sps_id==ss.shop_portfolio_shoe_id,sps_no_ss.sps_shop_id==ss.shop_id]
    sps_no_ss=sps_no_ss.join(ss,cond_sps_no_ss,how='left').drop('shop_id').drop('shop_group_id'). \
        withColumnRenamed('sps_shop_id','shop_id')
    return(sps_no_ss)


# def sps_with_no_ss(sps,ss):
#     sps = sps.alias("sps")
#     ss = ss.alias("ss")
#     cond_sps_ss = [ss.shop_portfolio_shoe_id==sps.id,ss.shop_id==sps.shop_id]
#     sps_no_ss=sps.join(ss,cond_sps_ss,'left_anti'). \
#     withColumnRenamed('id','sps_id').withColumnRenamed('shop_id','sps_shop_id').drop('business_name').drop('import_date').\
#     withColumnRenamed('updated_at','sps_updated_at').withColumnRenamed('created_at','sps_created_at')
#     sps_no_ss=sps_no_ss.select(sps_no_ss.columns[:14])
# #     cond_sps_ss_all = [ss_all.shop_portfolio_shoe_id==sps_no_ss.sps_id,ss_all.shop_id==sps_no_ss.sps_shop_id]
#     sps_no_ss=sps_no_ss.join(ss,cond_sps_ss,how='outer').drop('shop_id').drop('shop_group_id').\
#     withColumnRenamed('sps_shop_id','shop_id')
#     return(sps_no_ss)


#list of shoes & ss related to lives adults sps (updated today), trashed and with image
def shoes_trashed_to_check(sps,ss,shoes,srs):
    srs_kids=srs.filter(col('scale')==0).filter(col('size_numeric')<=34)
    sps_kids_ids=srs_kids.select(col('shop_portfolio_shoe_id')).distinct()
    cond_sps_srs = [sps_kids_ids.shop_portfolio_shoe_id==sps.id]
    sps_adults=sps.join(sps_kids_ids, cond_sps_srs, how='left_anti')

    sps = sps_adults.alias("sps")
    ss = ss.alias("ss")
    s = shoes.filter(col("trash")=='true').filter(col('main_image').isNotNull()).alias("s")
    cond_sps_ss = [ss.shop_portfolio_shoe_id==sps.id,ss.shop_id==sps.shop_id]
    ss_adults_trashed=sps.join(ss,cond_sps_ss).join(s,ss.shoe_id==s.id).select('ss.*'). \
        withColumnRenamed('id','ss_id').withColumnRenamed('created_at','ss_created_at'). \
        withColumnRenamed('updated_at','ss_updated_at').withColumnRenamed('trash','ss_trash'). \
        withColumnRenamed('targeted_count','ss_targeted_count')
    s = shoes.alias("s")
    ss_adults_trashed=shoes.join(ss_adults_trashed,ss.shoe_id==s.id)
    return(ss_adults_trashed)


#1 - set variables
import datetime
import argparse
spark = SparkSession.builder.appName("portfolio_stats_collector").getOrCreate();
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
# esdomain="https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com"
# #to process a complete month
# day_ini_str="2020-04-01"
# day_fin_str="2020-04-30"
# es_index_sufix="2020-04"
# day_ini_date=day_to_date(day_ini_str)
# day_fin_date=day_to_date(day_fin_str)


#2 read data
imp,sps,ss,shoes,srs,si=read_data(path, day_ini_date, day_fin_date)


#2.1 read configuration file with no active shop to remove from alerts
conf_file=read_conf(path_configuration,conf_filename)
conf_file=conf_file.select(conf_file.columns[:3])
newcolnames=["shop_id","business_name","portfolio_active"]
conf_file=rename_columns(conf_file,newcolnames)

#3 generate emr_stats_all with the metrics from emr stats of imports of period (yesterday bu default)
imp_emr=gen_imp_emr(imp)
emr_stats_all=gen_emr_stats_all(path_emr_imports,imp_emr)

#4 generate count_shoes_all with stats of sps, ss and shoes by shop and by day in the treated period  (yesterday by default)

# count_shoes_all = fun_count_shoes(sps_adults,ss,shoes_all,"sps_count","ss_count", "shoes_count")
count_shoes_all = fun_count_shoes(sps,ss,shoes,srs,"sps_count","ss_count","ss_trashed", "shoes_count","shoes_trashed","shoes_trashed_noimage","shoes_trashed_kids")
# count_shoes_all.show()

#5 generate count_new_shoes_all with new sps, ss and shoes by shop and by day in the treated period  (yesterday by default)
sps_new = select_range_time(sps,day_ini_date,day_fin_date)
ss_new = select_range_time(ss,day_ini_date,day_fin_date)
shoes_new = select_range_time(shoes,day_ini_date,day_fin_date)
count_new_shoes_all = fun_count_shoes(sps_new,ss_new,shoes_new,srs, "sps_new_count","ss_new_count","ss_new_trashed", "shoes_new_count","shoes_new_trashed","shoes_new_trashed_noimage","shoes_new_trashed_kids")
# count_new_shoes_all.show()


#6 generate count_images_all with the images by shop and by day in the treated period  (yesterday by default)
count_images_all = fun_count_images(sps_new,si,"sps_new_images_count")
# count_images_all.show()

#7 generate count_no_srs with the amount of sps with no size_run_sizes by shop and by day in the treated period  (yesterday by default)
# count_no_srs=fun_no_srs(sps, ss, shoes,srs, "sps_no_srs_count")
count_no_srs=fun_no_srs(sps, "sps_no_srs_count")
# count_no_srs.show()

#8 join results in portfolio_stat dataframe
portfolio_stats=emr_stats_all.join(count_shoes_all,["import_date","shop_id","business_name"],how='full')
portfolio_stats=portfolio_stats.join(count_new_shoes_all,["import_date","shop_id","business_name"],how='full')
portfolio_stats=portfolio_stats.join(count_images_all,["import_date","shop_id","business_name"],how='full')
portfolio_stats=portfolio_stats.join(count_no_srs,["import_date","shop_id","business_name"],how='full')
portfolio_stats = portfolio_stats.withColumn("import_id", col("import_id").cast(StringType()))
portfolio_stats=clean_null(portfolio_stats)


#9 add sps_expected
portfolio_stats=add_sps_expected(portfolio_stats)
# portfolio_stats.show()

#10 save in S3 and ES
df_write_es(portfolio_stats,esdomain,"portfolio_stats-"+es_index_sufix)
df_write_csv(portfolio_stats, path_output,"portfolio_stats",mode="append")



#11 alerts calculation
#calculate alerts only with 1 import a day (no keywords that broke the stats)
portfolio_stats_oneimp=portfolio_stats.filter(col('imports_count')==1)
keycols=['shop_id','business_name','import_date','import_id']

sps_errors_in_rules_df=alert_sps_errors_in_rules(portfolio_stats_oneimp, keycols, "sps_errors_in_rules")

less_sps_than_expected_df=alert_no_threshold(portfolio_stats_oneimp,keycols, "less_sps_than_expected", \
                                             "dif=%d sps=%d sps_expected=%d","sps_count","sps_expected")

less_ss_than_sps_df=alert_less_ss_than_sps(portfolio_stats_oneimp,keycols, "less_ss_than_sps", \
                                           "dif=%d ss=%d ss_trashed=%d sps=%d","ss_count","ss_trashed","sps_count")

less_sps_than_ss_df=alert_less_sps_than_ss(portfolio_stats_oneimp,keycols, "less_sps_than_ss", \
                                           "dif=%d  sps=%d ss=%d ss_trashed=%d","sps_count","ss_count", "ss_trashed")

less_sps_new_than_ss_new_df=alert_less_sps_than_ss(portfolio_stats_oneimp,keycols, "less_sps_new_than_ss_new", \
                                                   "dif=%d  sps_new=%d ss_new=%d ss_trashed=%d","sps_new_count","ss_new_count", "ss_new_trashed")

less_shoes_than_ss_df=alert_no_threshold(portfolio_stats_oneimp,keycols, "less_shoes_than_ss", \
                                         "dif=%d shoes=%d ss=%d","shoes_count","ss_count")


less_sps_new_images_than_expected_df=alert_no_threshold(portfolio_stats_oneimp,keycols, \
                                                        "less_sps_new_images_than_expected","dif=%d sps_new_images=%d sps_new=%d","sps_new_images_count","sps_new_count")

# less_sps_new_images_than_expected_df=alert_no_threshold(portfolio_stats_oneimp,keycols, \
#                         "less_sps_new_images_than_expected","dif=%d sps_new=%d sps_new_images=%d","sps_new_count","sps_new_images_count")
sps_withUnsetKeywords_df=alert_sps_withUnsetKeywords(portfolio_stats_oneimp, keycols, "sps_withUnsetKeywords")
sps_withEmptyBrandOrArticleNumber_df=alert_sps_withEmptyBrandOrArticleNumber(portfolio_stats_oneimp, keycols, \
                                                                             "sps_withEmptyBrandOrArticleNumber")
imp_not_finished_df=alert_imp_not_finished(imp,imp_emr,'imp_not_finished')
#can't use emR newShopPortfolioShoes: not only new shoes-
less_sps_new_than_expected_df=alert_no_threshold(portfolio_stats_oneimp,keycols, "less_sps_new_than_expected", \
                                                 "sps_new=%d sps_new_expected=%d","sps_new_count","newShopPortfolioShoes")

# less_ss_new_than_sps_new_df = alert_no_threshold(portfolio_stats_oneimp,keycols, "less_ss_new_than_sps_new",\
#                                                 "dif=%d ss_new=%d sps_new=%d","ss_new_count","sps_new_count")

less_ss_new_than_sps_new_df = alert_less_ss_than_sps(portfolio_stats_oneimp,keycols, "less_ss_new_than_sps_new", \
                                                     "dif=%d ss_new=%d ss_new_trashed sps_new=%d","ss_new_count","ss_new_trashed","sps_new_count")
sps_without_srs_df=alert_value_no_zero(portfolio_stats_oneimp, keycols,"count_no_srs","sps_without_srs")

trashed_shoes_to_check_df=alert_trashed_shoes(portfolio_stats_oneimp,keycols, "trashed_shoes_to_check", \
                                              "dif=%d shoes_trashed=%d no_image=%d kids=%d","shoes_trashed","shoes_trashed_noimage","shoes_trashed_kids")


trashed_new_shoes_to_check_df=alert_trashed_shoes(portfolio_stats_oneimp,keycols, "trashed_new_shoes_to_check", \
                                                  "dif=%d shoes_new_trashed=%d no_image=%d kids=%d","shoes_new_trashed","shoes_new_trashed_noimage","shoes_new_trashed_kids")



if not trashed_new_shoes_to_check_df.rdd.isEmpty():
    trashed_new_to_check_df = shoes_trashed_to_check(sps_new,ss_new,shoes_new,srs)
    report_trashed_new_to_check_df=trashed_new_shoes_to_check_df.join(trashed_new_to_check_df, ['shop_id'])
    df_write_csv(report_trashed_new_to_check_df, path_output,"report_trashed_new_to_check_df",mode="append")


if not less_ss_new_than_sps_new_df.rdd.isEmpty():
    sps_with_no_ss_df=sps_with_no_ss(sps_new,ss_new)
    report_less_ss_than_sps_df= less_ss_new_than_sps_new_df.join(sps_with_no_ss_df, ['shop_id'])
    df_write_csv(report_less_ss_than_sps_df, path_output,"report_less_ss_new_than_sps_new_df",mode="append")


# if less_shoes_than_ss_df.rdd.isEmpty():
#     ss_with_no_shoes_df=ss_with_no_shoes(sps,ss,shoes,shoes_all)
#     report_less_shoes_than_ss_df=less_shoes_than_ss_df.join(ss_with_no_shoes_df, ['shop_id'])
#     df_write_csv(report_less_shoes_than_ss_df, path_output,"report_less_shoes_than_ss_df",mode="append")

# less_sps_new_images_than_expected_df.show()

#12 merge alerts
portfolio_alert=merge_outputs(sps_errors_in_rules_df,less_sps_than_expected_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,less_ss_than_sps_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,less_sps_than_ss_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,less_shoes_than_ss_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,less_sps_new_images_than_expected_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,sps_withUnsetKeywords_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,sps_withEmptyBrandOrArticleNumber_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,imp_not_finished_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,less_ss_new_than_sps_new_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,sps_without_srs_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,trashed_new_shoes_to_check_df,keycols)
portfolio_alert=merge_outputs(portfolio_alert,less_sps_new_than_ss_new_df,keycols)
#13 exclude no active shops
portfolio_alert=exclude_no_active_shops(portfolio_alert,conf_file)


#14 generate alert summary and reord columns to print
portfolio_alert_columns = ['shop_id','business_name','import_date','import_id','alert_summary','imp_not_finished', \
                           'sps_errors_in_rules','sps_withEmptyBrandOrArticleNumber','sps_without_srs', \
                           'less_sps_new_images_than_expected','sps_withUnsetKeywords','trashed_new_shoes_to_check','less_sps_than_expected','less_ss_than_sps', \
                           'less_sps_than_ss','less_shoes_than_ss','less_ss_new_than_sps_new','less_sps_new_than_ss_new']

alert_cols = ['imp_not_finished','sps_errors_in_rules','sps_withEmptyBrandOrArticleNumber','sps_without_srs', \
              'less_sps_new_images_than_expected','sps_withUnsetKeywords','trashed_new_shoes_to_check','less_sps_than_expected','less_ss_than_sps', \
              'less_sps_than_ss','less_shoes_than_ss','less_ss_new_than_sps_new','less_sps_new_than_ss_new']

concat_cols = udf(concat_2arr, StringType())
portfolio_alert=portfolio_alert.withColumn("alerts",  F.array([F.lit(x) for x in alert_cols]))

portfolio_alert=portfolio_alert.withColumn("alert_summary", concat_cols(col('alerts'),array('imp_not_finished','sps_errors_in_rules', \
                                                                                            'sps_withEmptyBrandOrArticleNumber','sps_without_srs','less_sps_new_images_than_expected', \
                                                                                            'sps_withUnsetKeywords','trashed_new_shoes_to_check','less_sps_than_expected','less_ss_than_sps', \
                                                                                            'less_sps_than_ss','less_shoes_than_ss','less_ss_new_than_sps_new','less_sps_new_than_ss_new')))

portfolio_alert = portfolio_alert.select(portfolio_alert_columns).orderBy(col('business_name'))

#15 save alerts
# df_write_es(portfolio_alert,esdomain,"portfolio_alert-"+es_index_sufix)

df_write_csv(portfolio_alert, path_output,"portfolio_alert",mode="append")
