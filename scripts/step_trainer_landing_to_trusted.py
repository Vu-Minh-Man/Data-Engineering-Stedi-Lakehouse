import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated S3
CustomerCuratedS3_node1685627569118 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedS3_node1685627569118",
)

# Script generated for node Step Trainer Landing S3
StepTrainerLandingS3_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingS3_node1",
)

# Script generated for node Trusted Join
TrustedJoin_node1685627620657 = Join.apply(
    frame1=StepTrainerLandingS3_node1,
    frame2=CustomerCuratedS3_node1685627569118,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="TrustedJoin_node1685627620657",
)

# Script generated for node Select Fields
SelectFields_node1685634324027 = SelectFields.apply(
    frame=TrustedJoin_node1685627620657,
    paths=["serialNumber", "sensorReadingTime", "distanceFromObject"],
    transformation_ctx="SelectFields_node1685634324027",
)

# Script generated for node Step Trainer Trusted S3
StepTrainerTrustedS3_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFields_node1685634324027,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-gs/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrustedS3_node3",
)

job.commit()
