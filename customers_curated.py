import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1749842818995 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1749842818995")

# Script generated for node Customer Landing
CustomerLanding_node1749842790987 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="customer_landing", transformation_ctx="CustomerLanding_node1749842790987")

# Script generated for node Join
CustomerLanding_node1749842790987DF = CustomerLanding_node1749842790987.toDF()
AccelerometerLanding_node1749842818995DF = AccelerometerLanding_node1749842818995.toDF()
Join_node1749842840939 = DynamicFrame.fromDF(CustomerLanding_node1749842790987DF.join(AccelerometerLanding_node1749842818995DF, (CustomerLanding_node1749842790987DF['email'] == AccelerometerLanding_node1749842818995DF['user']), "left"), glueContext, "Join_node1749842840939")

# Script generated for node SQL Query
SqlQuery7632 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SQLQuery_node1749843074235 = sparkSqlQuery(glueContext, query = SqlQuery7632, mapping = {"myDataSource":Join_node1749842840939}, transformation_ctx = "SQLQuery_node1749843074235")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749843074235, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749841536896", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749843160169 = glueContext.getSink(path="s3://cd0091bucket/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749843160169")
AmazonS3_node1749843160169.setCatalogInfo(catalogDatabase="main_database",catalogTableName="customers_curated")
AmazonS3_node1749843160169.setFormat("json")
AmazonS3_node1749843160169.writeFrame(SQLQuery_node1749843074235)
job.commit()