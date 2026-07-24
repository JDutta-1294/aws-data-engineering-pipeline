import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit, count, upper,round,row_number
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.window import Window

# Read runtime arguments
args = getResolvedOptions(
                        sys.argv,
                        ["JOB_NAME",
                         "INPUT_PATH",
                         "OUTPUT_PATH",
                         "REJECT_PATH"]
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

    # Data Quality Checks
    logger.info("Starting data quality checks")
    orders_df.printSchema()

    
    orders_df=orders_df.withColumn("Price", col("Price").cast("double"))
    # Adding column "Rejection_Reason" to invalid records
    orders_df = orders_df.withColumn("Rejection_Reason",
                                     when(col("OrderID").isNull(),lit("Missing OrderID"))
                                     .when(col("Price").isNull(),lit("Price is not numeric"))
                                     .when(col("Price")<=0,lit("Invalid Price")))


    valid_conditions=((col("OrderID").isNotNull())
                      &
                      (col("Price").isNotNull())
                      &
                      (col("Price")>0)
                     )
    valid_records_df=orders_df.filter(valid_conditions)
    invalid_records_df=orders_df.filter(~valid_conditions)

    valid_records_count = valid_records_df.count()
    invalid_records_count = invalid_records_df.count()

    logger.info(f"Valid records count: {valid_records_count}")
    logger.info(f"Invalid records count: {invalid_records_count}")

    if valid_records_count == 0:
        logger.error("No valid record found. Failing the job.")
        raise Exception("Data quality checks failed. No valid records available.")

    if invalid_records_count > 0:
        logger.info(f"Writing invalid records to {args['REJECT_PATH']}")
        invalid_records_dyf=DynamicFrame.fromDF(invalid_records_df, glueContext, "Invalid Records")
        glueContext.write_dynamic_frame.from_options(
            frame=invalid_records_dyf,
            connection_type="s3",
            connection_options={"path": args["REJECT_PATH"]},
            format="parquet",
            transformation_ctx="Write_invalid_records"
        )

    #Busness Logic: Adding 10% discount to Price of valid records
    logger.info("Calculating discounted Price")
    valid_records_df = valid_records_df.withColumn("discounted_Price", round(col("Price")*0.9,2))
    logger.info("Calculating final Price after GST")
    valid_records_df = valid_records_df.withColumn("final_Price", round(col("discounted_Price")*1.18,2))

    #For every customer, identify whether this is their first order, second order, third order...
    logger.info("Calculating order number for each customer")
    window_spec = Window.partitionBy("Customer").orderBy("OrderDate")
    valid_records_df = valid_records_df.withColumn("order_number", row_number().over(window_spec))\
    .withColumn("is_first_order", when(col("order_number")==1, lit(True)).otherwise(lit(False)))

    # Writing curated data to output path
    logger.info(f"writing curated data to {args['OUTPUT_PATH']}")
    valid_records_dyf = DynamicFrame.fromDF(valid_records_df, glueContext, "Valid Records")
    glueContext.write_dynamic_frame.from_options(
        frame= valid_records_dyf,
        connection_type= "s3",
        format = "parquet",
        connection_options={"path": args["OUTPUT_PATH"],
                            "partitionKeys": ["OrderDate"]},
        format_options = { "compression": "snappy"},
        transformation_ctx = "orders_sink"
    )

    # Commit the job
    logger.info("Committing Glue Job")
    job.commit()
    logger.info("Orders ETL completed successfully")

except Exception as e:
    logger.error(f"Orders ETL Job failed: {str(e)}")
    raise 
