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
customer_curated_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://unique-bucket-name/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_df",
)

# Script generated for node Amazon S3
step_trainer_landing_df = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://unique-bucket-name/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_df",
)

# Script generated for node Join
joined_df = Join.apply(
    frame1=customer_curated_df,
    frame2=step_trainer_landing_df,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="joined_df",
)

# Script generated for node Drop Fields
cleaned_df = DropFields.apply(
    frame=joined_df,
    paths=[
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "customerName",
        "email",
        "phone",
        "birthDay",
    ],
    transformation_ctx="cleaned_df",
)

# Script generated for node S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://unique-bucket-name/step_trainer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="s3_output",
)

job.commit()
