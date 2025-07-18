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

# Script generated for node Customer Trusted
CustomerTrusted_node1749836978329 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1749836978329")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1749836823968 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1749836823968")

# Script generated for node Share with Research
SqlQuery7543 = '''
select user, timeStamp, x, y, z
from Accelerometer
join Customer
on Accelerometer.user = Customer.email;
'''
SharewithResearch_node1749837585572 = sparkSqlQuery(glueContext, query = SqlQuery7543, mapping = {"Customer":CustomerTrusted_node1749836978329, "Accelerometer":AccelerometerLanding_node1749836823968}, transformation_ctx = "SharewithResearch_node1749837585572")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1749837585572, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749834965556", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1749837598821 = glueContext.getSink(path="s3://cd0091bucket/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1749837598821")
AccelerometerTrusted_node1749837598821.setCatalogInfo(catalogDatabase="main_database",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1749837598821.setFormat("json")
AccelerometerTrusted_node1749837598821.writeFrame(SharewithResearch_node1749837585572)
job.commit()