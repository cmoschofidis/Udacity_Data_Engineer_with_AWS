
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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1676072957579 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1676072957579",
)

# Script generated for node Machine Learning Filter
MachineLearningFilter_node2 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1676072957579,
    keys1=["customername"],
    keys2=["user"],
    transformation_ctx="MachineLearningFilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1676073014820 = DropFields.apply(
    frame=MachineLearningFilter_node2,
    paths=["customername"],
    transformation_ctx="DropFields_node1676073014820",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1676073128583 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676073014820,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-human-bl-analytics/step_trainer/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1676073128583",
)

job.commit()