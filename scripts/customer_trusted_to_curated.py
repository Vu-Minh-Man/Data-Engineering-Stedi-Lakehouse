import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted S3
CustomerTrustedS3_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedS3_node1",
)

# Script generated for node Accelerometer Trusted S3
AccelerometerTrustedS3_node1685538318299 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lakehouse-gs/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedS3_node1685538318299",
    )
)

# Script generated for node Curated Join
CuratedJoin_node1685538439670 = Join.apply(
    frame1=AccelerometerTrustedS3_node1685538318299,
    frame2=CustomerTrustedS3_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CuratedJoin_node1685538439670",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685538525612 = DynamicFrame.fromDF(
    CuratedJoin_node1685538439670.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1685538525612",
)

# Script generated for node Drop Fields
DropFields_node1685538569077 = DropFields.apply(
    frame=DropDuplicates_node1685538525612,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1685538569077",
)

# Script generated for node Customer Curated S3
CustomerCuratedS3_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685538569077,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-gs/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedS3_node3",
)

job.commit()
