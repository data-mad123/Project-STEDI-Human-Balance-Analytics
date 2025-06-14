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
AccelerometerTrusted_node1749846812447 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1749846812447")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1749846781343 = glueContext.create_dynamic_frame.from_catalog(database="main_database", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1749846781343")

# Script generated for node SQL Query
SqlQuery7402 = '''
select *
from Step_Trainer
join Accelerometer
on Step_Trainer.sensorreadingtime = Accelerometer.timestamp;
'''
SQLQuery_node1749847139153 = sparkSqlQuery(glueContext, query = SqlQuery7402, mapping = {"Step_Trainer":StepTrainerTrusted_node1749846781343, "Accelerometer":AccelerometerTrusted_node1749846812447}, transformation_ctx = "SQLQuery_node1749847139153")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1749847139153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749846380773", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749847888934 = glueContext.getSink(path="s3://cd0091bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1749847888934")
AmazonS3_node1749847888934.setCatalogInfo(catalogDatabase="main_database",catalogTableName="machine_learning_curated")
AmazonS3_node1749847888934.setFormat("json")
AmazonS3_node1749847888934.writeFrame(SQLQuery_node1749847139153)
job.commit()