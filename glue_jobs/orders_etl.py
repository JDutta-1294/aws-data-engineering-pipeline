import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job


# Read runtime arguments
args= getResolvedOptions(sys.argv,["JOB_NAME","INPUT_PATH","OUTPUT_PATH"])

# Create Spark Context
sc = SparkContext()
# Create Glue Context
gluecontext=GlueContext(sc)
# Create Spark Session
spark=gluecontext.spark_session

#Logger
logger=gluecontext.get_logger()
# Create Glue Job
job=Job(gluecontext)
job.init(args["JOB_NAME"],args)

