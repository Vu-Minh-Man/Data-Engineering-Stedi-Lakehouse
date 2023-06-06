import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
import re


# Script generated for node Customer Invalid Data Filter Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # get data from first node
    customerDf = dfc.select(list(dfc.keys())[0]).toDF()

    # keep the last registered email
    customerDf = customerDf.orderBy(
        ["email", "registrationDate"], ascending=[True, False]
    )
    customerDf = customerDf.dropDuplicates(["email"])

    newCustomerDf = DynamicFrame.fromDF(customerDf, glueContext, "newCustomerDf")
    return DynamicFrameCollection({"CustomTransform0": newCustomerDf}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing S3
CustomerLandingS3_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-gs/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingS3_node1",
)

# Script generated for node Customer Invalid Data Filter Transform
CustomerInvalidDataFilterTransform_node1685511110609 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"CustomerLandingS3_node1": CustomerLandingS3_node1}, glueContext
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1685512041797 = SelectFromCollection.apply(
    dfc=CustomerInvalidDataFilterTransform_node1685511110609,
    key=list(CustomerInvalidDataFilterTransform_node1685511110609.keys())[0],
    transformation_ctx="SelectFromCollection_node1685512041797",
)

# Script generated for node Trusted Filter
TrustedFilter_node1685281327760 = Filter.apply(
    frame=SelectFromCollection_node1685512041797,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="TrustedFilter_node1685281327760",
)

# Script generated for node Customer Trusted S3
CustomerTrustedS3_node3 = glueContext.write_dynamic_frame.from_options(
    frame=TrustedFilter_node1685281327760,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-gs/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrustedS3_node3",
)

job.commit()
