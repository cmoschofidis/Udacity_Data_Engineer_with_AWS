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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1683038087155 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1683038087155",
)

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLandingZone_node1",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node2 = Join.apply(
    frame1=AccelerometerLandingZone_node1,
    frame2=CustomerTrustedZone_node1683038087155,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1683038293993 = DropFields.apply(
    frame=CustomerPrivacyFilter_node2,
    paths=["timestamp", "user", "x", "y", "z"],
    transformation_ctx="DropFields_node1683038293993",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1683040331445 = DynamicFrame.fromDF(
    DropFields_node1683038293993.toDF().dropDuplicates(["email", "serialnumber"]),
    glueContext,
    "DropDuplicates_node1683040331445",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1683040331445,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-bl-analytics/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrustedZone_node3",
)

job.commit()