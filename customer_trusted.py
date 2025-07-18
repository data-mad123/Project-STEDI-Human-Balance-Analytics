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

# Script generated for node Customer Landing
CustomerLanding_node1749835296167 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="customer_landing", transformation_ctx="CustomerLanding_node1749835296167")

# Script generated for node Share with Research
SqlQuery7085 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SharewithResearch_node1749835368608 = sparkSqlQuery(glueContext, query = SqlQuery7085, mapping = {"myDataSource":CustomerLanding_node1749835296167}, transformation_ctx = "SharewithResearch_node1749835368608")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1749835368608, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749834965556", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749835942491 = glueContext.getSink(path="s3://cd0091bucket/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749835942491")
AmazonS3_node1749835942491.setCatalogInfo(catalogDatabase="main_database",catalogTableName="customer_trusted")
AmazonS3_node1749835942491.setFormat("json")
AmazonS3_node1749835942491.writeFrame(SharewithResearch_node1749835368608)
job.commit()