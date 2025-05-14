# Changing Values
rds_secret_name = "rds-varchasv"
rds_jdbc_url = "jdbc:mysql://grp-3-rds-varchasv.ck9seaek8jue.us-east-1.rds.amazonaws.com/grp_3"
rds_table_name = "products"
region_name = 'us-east-1'

redshift_secret_name = "redshift-varchasv"
redshift_jdbc_url = "jdbc:redshift://vt-grp-3-redshift.crwl5wvff7ol.us-east-1.redshift.amazonaws.com:5439/varchasv"

############################# Read from RDS ##############################
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
import json
from botocore.exceptions import ClientError
# Function to retrieve secrets from AWS Secrets Manager
def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    secret = json.loads(response['SecretString'])
    return secret

# Create Spark Session
spark = SparkSession.builder.appName("RDS_to_Redshift_ETL").getOrCreate()


# Fetch RDS Credentials securely
rds_secrets = get_secret(rds_secret_name, region_name)
rds_url = rds_jdbc_url
rds_properties = {
    "user": rds_secrets["username"],
    "password": rds_secrets["password"],
    "driver": "com.mysql.cj.jdbc.Driver"
}
# Read Data from RDS
df = spark.read.jdbc(url=rds_url, table=rds_table_name, properties=rds_properties)
 
# Transform Data (Example: Filtering & Aggregating)
newDF = df.groupBy('material').agg(count(col('product_id')).alias('number_of_products'))
newDF1 = df.filter(col('price')!='N/A').groupBy('frame_type').agg(round(avg(col('price').cast('float')),2).alias('total_price'))
newDF2 = df.withColumn("stock_availability", col("stock_availability").cast("string"))\
    .groupBy('stock_availability').agg(count(col('product_id')).alias('number_of_products'))\
    .withColumn('stock_availability', when(col('stock_availability') == "true", "In Stock")\
    .when(col('stock_availability') == "false", "Out of Stock").otherwise(col('stock_availability')))
############################# Load to Redshift ##############################
# Fetch Redshift Credentials securely
redshift_secrets = get_secret(redshift_secret_name, region_name)
redshift_url = redshift_jdbc_url
redshift_properties = {
    "user": redshift_secrets["username"],
    "password": redshift_secrets["password"],
    "driver": "com.amazon.redshift.jdbc.Driver"
}
 
# Load Data into Redshift Tables
newDF.write.jdbc(url=redshift_url, table="material_summary", mode="overwrite", properties=redshift_properties)
newDF1.write.jdbc(url=redshift_url, table="frame_price_summary", mode="overwrite", properties=redshift_properties)
newDF2.write.jdbc(url=redshift_url, table="stock_availability_summary", mode="overwrite", properties=redshift_properties)

print("Data successfully loaded from RDS to Redshift securely!")
 
