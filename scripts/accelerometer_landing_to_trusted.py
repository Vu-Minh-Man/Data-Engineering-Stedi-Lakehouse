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

# Script generated for node Customer Trusted S3
CustomerTrustedS3_node1685323956683 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedS3_node1685323956683",
)

# Script generated for node Accelerometer Landing S3
AccelerometerLandingS3_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingS3_node1",
)

# Script generated for node Trusted Join
TrustedJoin_node1685323891902 = Join.apply(
    frame1=AccelerometerLandingS3_node1,
    frame2=CustomerTrustedS3_node1685323956683,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="TrustedJoin_node1685323891902",
)

# Script generated for node Drop Fields
DropFields_node1685324207895 = DropFields.apply(
    frame=TrustedJoin_node1685323891902,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1685324207895",
)

# Script generated for node Accelerometer Trusted S3
AccelerometerTrustedS3_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685324207895,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-gs/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrustedS3_node3",
)

job.commit()
