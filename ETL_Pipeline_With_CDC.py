from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
import time


CASSANDRA_KEYSPACE = "spark_streams"
CASSANDRA_TABLE = "created_jobs"

MYSQL_HOST = "localhost"
MYSQL_DATABASE = "Data_Warehouse"

spark = (
    SparkSession.builder
    .config("spark.jars.packages", 
            "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,"
            "com.mysql:mysql-connector-j:8.0.33")
    .getOrCreate()
)


def read_company_df():
    dbtable = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company_df = read_data_from_mysql(dbtable)
    return company_df

def read_data_from_mysql(dbtable):
    df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://{host}:3306/{db}".format(host=MYSQL_HOST, db=MYSQL_DATABASE)) \
    .option("dbtable", dbtable) \
    .option("user", "root") \
    .option("password", "1") \
    .load()

    return df

def get_latest_time_cassandra():
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=CASSANDRA_TABLE,keyspace=CASSANDRA_KEYSPACE).load()
    cassandra_latest = df.agg({'ts':'max'}).take(1)[0][0].strftime('%Y-%m-%d %H:%M:%S')
    return cassandra_latest


def get_latest_time_mysql():
    sql = """(select max(latest_modified_time) from events) data"""
#     df = read_data_from_mysql(sql)
#     mysql_latest = df.select(F.max('latest_modified_time')).collect()[0][0]
#     mysql_latest = mysql_latest.strftime('%Y-%m-%d %H:%M:%S')
#     return mysql_latest
    mysql_time = read_data_from_mysql(sql)
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest 

def import_to_mysql(data, dbtable):
    data.write.format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://{host}:3306/{db}".format(host=MYSQL_HOST, db=MYSQL_DATABASE)) \
    .option("dbtable", dbtable) \
    .mode("append") \
    .option("user", "root") \
    .option("password", "1") \
    .save()

    return print('Import data to mysql')

def handle_click(data):
    click_data = data.filter(data.custom_track == 'click')
    click_data = click_data.na.fill({'job_id':0})
    click_data = click_data.na.fill({'custom_track':0})
    click_data = click_data.na.fill({'bid':0})
    click_data = click_data.na.fill({'campaign_id':0})
    click_data = click_data.na.fill({'group_id':0})
    click_data = click_data.na.fill({'publisher_id':0})

    click_data.createTempView('clicktable')
    click_output = spark.sql("""SELECT job_id, date(ts) as dates, hour(ts) as hours, group_id, campaign_id, publisher_id, Round(AVG(bid),2) as bid_set, count(*) as clicks, sum(bid) as spend_hour 
                       FROM clicktable 
                       GROUP BY job_id, campaign_id, group_id, publisher_id, date(ts), hour(ts)""")
    spark.catalog.dropTempView("clicktable")
    return click_output

def handle_conversion(data):
    conversion_data = data.filter(data.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'custom_track':0})
    conversion_data = conversion_data.na.fill({'bid':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    
    conversion_data.createTempView('conversiontable')
    conversion_output =  spark.sql("""SELECT job_id, date(ts) as dates, hour(ts) as hours, group_id, campaign_id, publisher_id, Round(AVG(bid),2) as bid_set, count(*) as conversion, sum(bid) as spend_hour 
                       FROM conversiontable 
                       GROUP BY job_id, campaign_id, group_id, publisher_id, date(ts), hour(ts)""")
    spark.catalog.dropTempView("conversiontable")
    return conversion_output

def handle_qualified(data):
    qualified_data = data.filter(data.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'custom_track':0})
    qualified_data = qualified_data.na.fill({'bid':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    
    qualified_data.createTempView('qualifiedtable')
    qualified_output = spark.sql("""SELECT job_id, date(ts) as dates, hour(ts) as hours, group_id, campaign_id, publisher_id, Round(AVG(bid),2) as bid_set, count(*) as qualified, sum(bid) as spend_hour 
                       FROM qualifiedtable 
                       GROUP BY job_id, campaign_id, group_id, publisher_id, date(ts), hour(ts)""")
    spark.catalog.dropTempView("qualifiedtable")
    return qualified_output

def handle_unqualified(data):
    unqualified_data = data.filter(data.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'custom_track':0})
    unqualified_data = unqualified_data.na.fill({'bid':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    
    unqualified_data.createTempView('unqualifiedtable')
    unqualified_output = spark.sql("""SELECT job_id, date(ts) as dates, hour(ts) as hours, group_id, campaign_id, publisher_id, Round(AVG(bid),2) as bid_set, count(*) as unqualified, sum(bid) as spend_hour 
                       FROM unqualifiedtable 
                       GROUP BY job_id, campaign_id, group_id, publisher_id, date(ts), hour(ts)""")
    spark.catalog.dropTempView("unqualifiedtable")
    return unqualified_output

def handle_na_customtrack(data):
    data = data.na.fill({'clicks':'0'})
    data = data.na.fill({'conversion':'0'})
    data = data.na.fill({'qualified_application':'0'})
    data = data.na.fill({'disqualified_application':'0'})
    data = data.withColumn('sources', lit('Cassandra'))
    return data
    

def handle_merge(click_output, conversion_output, qualified_output, unqualified_output):
    df = click_output.join(conversion_output,['job_id','dates','hours','publisher_id','campaign_id','group_id', 'bid_set', 'spend_hour'],'full').\
        join(qualified_output,['job_id','dates','hours','publisher_id','campaign_id','group_id', 'bid_set', 'spend_hour'],'full').\
        join(unqualified_output,['job_id','dates','hours','publisher_id','campaign_id','group_id', 'bid_set', 'spend_hour'],'full')
    df = df.withColumnRenamed('qualified', 'qualified_application').withColumnRenamed('unqualified', 'disqualified_application')

    return df

def process_cassandra_data(data):
    click_output = handle_click(data)
    conversion_output = handle_conversion(data)
    qualified_output = handle_qualified(data)
    unqualified_output = handle_unqualified(data)
    
    final_data = handle_merge(click_output, conversion_output, qualified_output, unqualified_output)
    final_data = handle_na_customtrack(final_data)

    cassandra_time = get_latest_time_cassandra()
    final_data = final_data.withColumn('latest_modified_time', F.lit(cassandra_time))
    return final_data

def main_task(mysql_time):
    print('-----------------------------')
    print('Retrieving data from Cassandra')
    print('-----------------------------')
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=CASSANDRA_TABLE,keyspace=CASSANDRA_KEYSPACE).load().where(col('ts')>= mysql_time)
    print('-----------------------------')
    print('Selecting data from Cassandra')
    print('-----------------------------')
    data = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    data = data.filter((data.job_id.isNotNull()) & (data.custom_track.isNotNull()))

    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    cassandra_output = process_cassandra_data(data)
    print('-----------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    print('Finalizing Output')
    print('-----------------------------')
    company_df = read_company_df()
    final_output = cassandra_output.join(company_df,'job_id','left').drop(company_df.campaign_id).drop(company_df.group_id)
    
    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output, "events")

def main():
    while True:
        start = time.time()

        cassandra_time = get_latest_time_cassandra()
        mysql_time = get_latest_time_mysql()

        print("Latest time in Cassandra is: {}".format(cassandra_time))
        print("Latest time in MySQL is: {}".format(mysql_time))
        if (cassandra_time > mysql_time):
            main_task(mysql_time)
            print("New data found")
        else:
            print("No New data found!")

        end = time.time()
        print("Job takes {} seconds to execute".format(end - start))

        time.sleep(60)

main()
