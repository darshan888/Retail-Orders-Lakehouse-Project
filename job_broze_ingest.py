import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "target_bucket", "source_database"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["target_bucket"]
SRC_DB = args["source_database"]
TABLES = ["orders", "customers", "products"]

for tbl in TABLES:
    # Job bookmarks make this incremental: files already ingested are skipped.
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SRC_DB,
        table_name=tbl,
        transformation_ctx=f"src_{tbl}",
    )
    if dyf.count() == 0:
        print(f"[bronze] {tbl}: nothing new to ingest, skipping")
        continue

    df = dyf.toDF()

    # Bronze contract: faithful copy. Normalize column NAMES only, never values.
    # Everything stays string so a bad value can never fail ingestion.
    df = df.select([F.col(c).cast("string").alias(c.strip().lower()) for c in df.columns])

    df = (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.input_file_name())
          .withColumn("ingest_date", F.date_format(F.current_date(), "yyyy-MM-dd"))
    )

    sink = glueContext.getSink(
        path=f"s3://{BUCKET}/bronze/{tbl}/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["ingest_date"],
        compression="snappy",
        enableUpdateCatalog=True,          # creates/updates the table, no crawler needed
        transformation_ctx=f"sink_{tbl}",
    )
    sink.setCatalogInfo(catalogDatabase="retail_bronze", catalogTableName=tbl)
    sink.setFormat("glueparquet")
    sink.writeFrame(DynamicFrame.fromDF(df, glueContext, f"bronze_{tbl}"))
    print(f"[bronze] {tbl}: wrote {df.count()} rows")

job.commit()