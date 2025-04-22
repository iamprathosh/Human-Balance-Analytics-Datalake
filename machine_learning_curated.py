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

# Script generated for node S3 bucket
accelerometer_trusted_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://unique-bucket-name/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_df",
)

# Script generated for node Amazon S3
step_trainer_trusted_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://unique-bucket-name/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_df",
)

# Script generated for node Join
joined_df = Join.apply(
    frame1=accelerometer_trusted_df,
    frame2=step_trainer_trusted_df,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="joined_df",
)

# Script generated for node Drop Fields
cleaned_df = DropFields.apply(
    frame=joined_df,
    paths=["user"],
    transformation_ctx="cleaned_df",
)

# Script generated for node S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://unique-bucket-name/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="s3_output",
)

job.commit()
