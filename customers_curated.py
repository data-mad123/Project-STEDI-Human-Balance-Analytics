import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1749842818995 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1749842818995")

# Script generated for node Customer Trusted
CustomerTrusted_node1749842790987 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1749842790987")

# Script generated for node SQL Query
SqlQuery7421 = '''
select distinct Customer.*
from Customer
inner join Accelerometer
on Customer.email = Accelerometer.user;
'''
SQLQuery_node1749843074235 = sparkSqlQuery(glueContext, query = SqlQuery7421, mapping = {"Customer":CustomerTrusted_node1749842790987, "Accelerometer":AccelerometerTrusted_node1749842818995}, transformation_ctx = "SQLQuery_node1749843074235")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749843074235, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749841536896", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749843160169 = glueContext.getSink(path="s3://cd0091bucket/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749843160169")
AmazonS3_node1749843160169.setCatalogInfo(catalogDatabase="main_database",catalogTableName="customers_curated")
AmazonS3_node1749843160169.setFormat("json")
AmazonS3_node1749843160169.writeFrame(SQLQuery_node1749843074235)
job.commit()