import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job


# Read runtime arguments
args = getResolvedOptions(
                        sys.argv,
                        ["JOB_NAME",
                         "INPUT_PATH",
                         "OUTPUT_PATH"]
                         )

# Create Spark Context
sc = SparkContext()
# Create Glue Context
glueContext = GlueContext(sc)
# Create Spark Session
spark = glueContext.spark_session

#Logger
logger = glueContext.get_logger()
# Initialize Glue Job
job = Job(glueContext)
job.init(args["JOB_NAME"],args)

logger.info("Orders ETL Job started")

try:
    logger.info(f"Reading input data from {args['INPUT_PATH']}")

    orders_dyf = glueContext.create_dynamic_frame.from_options(
                       connection_type="s3",
                       format="csv",
                       connection_options={
                           "paths":[args["INPUT_PATH"]],
                           "recurse":True
                       },
                       format_options={
                           "withHeader":True,
                           "separator":","

                       },
                       transformation_ctx="orders_source"
                       )

    orders_df = orders_dyf.toDF()
    logger.info(f"Successfully read {orders_df.count()} records")

except Exception as e:
    logger.error(f"Failed to read input data: {str(e)}")
    raise 
