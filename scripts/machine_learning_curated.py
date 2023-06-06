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

# Script generated for node Accelerometer Trusted S3
AccelerometerTrustedS3_node1685629102057 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lakehouse-gs/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedS3_node1685629102057",
    )
)

# Script generated for node Customer Curated S3
CustomerCuratedS3_node1685629169346 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedS3_node1685629169346",
)

# Script generated for node Step Trainer Trusted S3
StepTrainerTrustedS3_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrustedS3_node1",
)

# Script generated for node Customer Join
CustomerJoin_node1685629219452 = Join.apply(
    frame1=AccelerometerTrustedS3_node1685629102057,
    frame2=CustomerCuratedS3_node1685629169346,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerJoin_node1685629219452",
)

# Script generated for node Accelerometer Drop Fields
AccelerometerDropFields_node1685629344414 = DropFields.apply(
    frame=CustomerJoin_node1685629219452,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="AccelerometerDropFields_node1685629344414",
)

# Script generated for node Accelerometer - Step Trainer Join
AccelerometerStepTrainerJoin_node1685629450733 = Join.apply(
    frame1=StepTrainerTrustedS3_node1,
    frame2=AccelerometerDropFields_node1685629344414,
    keys1=["serialNumber", "sensorReadingTime"],
    keys2=["serialNumber", "timeStamp"],
    transformation_ctx="AccelerometerStepTrainerJoin_node1685629450733",
)

# Script generated for node Select Fields
SelectFields_node1685630944320 = SelectFields.apply(
    frame=AccelerometerStepTrainerJoin_node1685629450733,
    paths=[
        "serialNumber",
        "sensorReadingTime",
        "distanceFromObject",
        "z",
        "user",
        "y",
        "x",
    ],
    transformation_ctx="SelectFields_node1685630944320",
)

# Script generated for node Machine Learning Curated S3
MachineLearningCuratedS3_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFields_node1685630944320,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-gs/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCuratedS3_node3",
)

job.commit()
