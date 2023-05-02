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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1683038087155 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_analytics",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1683038087155",
)

# Script generated for node Step Trainer node
StepTrainernode_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-bl-analytics/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainernode_node1",
)

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node2 = Join.apply(
    frame1=StepTrainernode_node1,
    frame2=CustomerTrustedZone_node1683038087155,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="CustomerPrivacyFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1683038293993 = DropFields.apply(
    frame=CustomerPrivacyFilter_node2,
    paths=[
        "sharewithpublicasofdate",
        "serialnumber",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1683038293993",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1683038293993,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-bl-analytics/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrustedZone_node3",
)

job.commit()