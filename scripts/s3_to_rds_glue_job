import boto3
import json
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# S3 and RDS Configuration
bkt_path = "s3://grp-3-bkt-varchasv/client_data/missing_field_detection.json"
secretName = "rds-varchasv"
regionName = "us-east-1"
rds_url = "jdbc:mysql://grp-3-rds-varchasv.ck9seaek8jue.us-east-1.rds.amazonaws.com/grp_3"
rds_table = "products"

# Create Spark Session
def create_session():
    spark = SparkSession.builder \
        .appName("Extract_from_s3_Load_to_RDS") \
        .getOrCreate()
    return spark

# Define Data Schema
def create_schema():
    return StructType([
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("frame_type", StringType()),
        StructField("material", StringType()),
        StructField("lens_type", StringType()),
        StructField("colors_available", ArrayType(StringType())),
        StructField("price", StringType()),
        StructField("stock_availability", BooleanType()),
        StructField("dimensions", StructType([
            StructField("lens_width", StringType()),
            StructField("bridge_width", StringType()),
            StructField("temple_length", StringType())
        ])),
        StructField("manufacturer", StructType([
            StructField("name", StringType()),
            StructField("location", StringType())
        ]))
    ])

# Load Data from S3
def load_data(spark, bkt_path):
    schema = create_schema()
    df = spark.read.json(bkt_path, multiLine=True, schema=schema)
    return df

# Clean Data & Handle Missing Fields
def clean_data(df):
    clean_df = df.withColumn("product_id", when(col("product_id").isNull() | (trim(col("product_id")) == ""), "N/A").otherwise(col("product_id")))
    clean_df = clean_df.withColumn("name", when(col("name").isNull() | (trim(col("name")) == ""), "N/A").otherwise(col("name")))
    clean_df = clean_df.withColumn("frame_type", when(col("frame_type").isNull() | (trim(col("frame_type")) == ""), "N/A").otherwise(col("frame_type")))
    clean_df = clean_df.withColumn("lens_type", when(col("lens_type").isNull() | (trim(col("lens_type")) == ""), "N/A").otherwise(col("lens_type")))
    clean_df = clean_df.withColumn("colors_available", 
    when(col("colors_available").isNull(), array(lit("N/A")))
    .otherwise(col("colors_available")))
    clean_df = clean_df.withColumn("stock_availability", 
    when(col("stock_availability").isNull(), lit("N/A"))  # Replace null values with "N/A"
    .otherwise(col("stock_availability").cast("string"))  # Convert BOOLEAN to STRING
)
    clean_df = clean_df.withColumn("price", when(col("price").isNull() | (trim(col("price")) == "") | (col('price') == 'N/A'), "0.0").otherwise(col("price").cast('float')))

    clean_df = clean_df.withColumn("dimensions", struct(
        when(col("dimensions.lens_width").isNull() | (trim(col("dimensions.lens_width")) == ""), "N/A").otherwise(col("dimensions.lens_width")).alias("lens_width"),
        when(col("dimensions.bridge_width").isNull() | (trim(col("dimensions.bridge_width")) == ""), "N/A").otherwise(col("dimensions.bridge_width")).alias("bridge_width"),
        when(col("dimensions.temple_length").isNull() | (trim(col("dimensions.temple_length")) == ""), "N/A").otherwise(col("dimensions.temple_length")).alias("temple_length")
    ))


    # Extract Nested Fields & Rename them for MySQL
    clean_df = clean_df.select(
       col("product_id"),
       col("name"),
       col("frame_type"),
       col("material"),
       col("lens_type"),
       to_json(col("colors_available")).alias("colors_available"), 
       col("price"),
       col("stock_availability"),
       col("dimensions.lens_width").alias("lens_width"),
       col("dimensions.bridge_width").alias("bridge_width"),
       col("dimensions.temple_length").alias("temple_length"),
       col("manufacturer.name").alias("manufacturer_name"),
       col("manufacturer.location").alias("manufacturer_location")
    )

    return clean_df

# Fetch AWS Secrets for Secure Authentication
def get_secret():
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=regionName)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secretName)
    except ClientError as e:
        raise e
    
    secret = json.loads(get_secret_value_response["SecretString"])
    return secret

# Fetch credentials securely
secrets = get_secret()
db_properties = {
    "user": secrets["username"],
    "password": secrets["password"],
    "driver": "com.mysql.cj.jdbc.Driver"
}

# RDS MySQL Configuration
db_url = rds_url
db_table = rds_table

# Write Data to RDS
def write_to_rds(df, db_url, db_table, db_properties):
    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", db_table) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .mode("overwrite") \
        .save()

# Execute Pipeline
spark = create_session()
df = load_data(spark, bkt_path)
clean_df = clean_data(df)

write_to_rds(clean_df, rds_url, rds_table, db_properties)
