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

# Script generated for node Customer Curated
CustomerCurated_node1749846392271 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="customers_curated", transformation_ctx="CustomerCurated_node1749846392271")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1749846781343 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1749846781343")

# Script generated for node SQL Query
SqlQuery7767 = '''
select distinct Step_Trainer.*
from Step_Trainer
join Customer
on Step_Trainer.serialnumber = Customer.serialnumber;
'''
SQLQuery_node1749847139153 = sparkSqlQuery(glueContext, query = SqlQuery7767, mapping = {"Customer":CustomerCurated_node1749846392271, "Step_Trainer":StepTrainerLanding_node1749846781343}, transformation_ctx = "SQLQuery_node1749847139153")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749847139153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749846380773", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749847888934 = glueContext.getSink(path="s3://cd0091bucket/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749847888934")
AmazonS3_node1749847888934.setCatalogInfo(catalogDatabase="main_database",catalogTableName="step_trainer_trusted")
AmazonS3_node1749847888934.setFormat("json")
AmazonS3_node1749847888934.writeFrame(SQLQuery_node1749847139153)
job.commit()