import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "target_bucket"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

BUCKET = args["target_bucket"]


def write_silver(df, table, partitions):
    """Full-refresh write: purge the prefix, then rewrite and update the catalog."""
    path = f"s3://{BUCKET}/silver/{table}/"
    glueContext.purge_s3_path(path, {"retentionPeriod": 0})
    sink = glueContext.getSink(
        path=path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partitions,
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx=f"silver_{table}",
    )
    sink.setCatalogInfo(catalogDatabase="retail_silver", catalogTableName=table)
    sink.setFormat("glueparquet")
    sink.writeFrame(DynamicFrame.fromDF(df, glueContext, table))
    print(f"[silver] {table}: wrote {df.count()} rows")


def blank_to_null(col):
    return F.when(F.trim(F.col(col)) == "", None).otherwise(F.trim(F.col(col)))


# dim_customers
dim_customers = (
    spark.table("retail_bronze.customers")
    .withColumn("customer_id", F.upper(blank_to_null("customer_id")))
    .withColumn("customer_name", F.initcap(blank_to_null("customer_name")))
    .withColumn("email", F.lower(blank_to_null("email")))
    .withColumn("country", F.upper(blank_to_null("country")))
    .withColumn("city", F.initcap(blank_to_null("city")))
    .withColumn("segment", F.upper(blank_to_null("segment")))
    .withColumn("signup_date", F.to_date(F.trim("signup_date"), "yyyy-MM-dd"))
    .fillna({"country": "UNKNOWN", "segment": "UNKNOWN"})
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .select("customer_id", "customer_name", "email", "country", "city",
            "signup_date", "segment")
)
write_silver(dim_customers, "dim_customers", [])

# dim_products
dim_products = (
    spark.table("retail_bronze.products")
    .withColumn("product_id", F.upper(blank_to_null("product_id")))
    .withColumn("product_name", F.trim("product_name"))
    .withColumn("category", F.initcap(blank_to_null("category")))
    .withColumn("subcategory", F.initcap(blank_to_null("subcategory")))
    .withColumn("cost_price", F.col("cost_price").cast("decimal(12,2)"))
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
    .select("product_id", "product_name", "category", "subcategory", "cost_price")
)
write_silver(dim_products, "dim_products", [])

# fact_orders
typed = (
    spark.table("retail_bronze.orders")
    .withColumn("order_id", F.upper(blank_to_null("order_id")))
    .withColumn("customer_id", F.upper(blank_to_null("customer_id")))
    .withColumn("product_id", F.upper(blank_to_null("product_id")))
    .withColumn("order_date", F.coalesce(
        F.to_date(F.trim("order_date"), "yyyy-MM-dd"),
        F.to_date(F.trim("order_date"), "MM/dd/yyyy"),
        F.to_date(F.trim("order_date"), "dd-MM-yyyy"),
    ))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    .withColumn("status", F.upper(blank_to_null("status")))
    .withColumn("channel", F.lower(blank_to_null("channel")))
)

w = Window.partitionBy("order_id").orderBy(F.col("_ingest_ts").desc(),
                                           F.col("_source_file").desc())
deduped = (
    typed.filter(F.col("order_id").isNotNull())
         .withColumn("_rn", F.row_number().over(w))
         .filter(F.col("_rn") == 1)
         .drop("_rn")
)

is_valid = (
    F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
    & F.col("order_date").isNotNull()
    & F.col("quantity").isNotNull() & (F.col("quantity") > 0)
    & F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0)
)

fact_orders = (
    deduped.filter(is_valid)
    .withColumn("gross_amount",
                (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)"))
    .withColumn("is_revenue", F.col("status").isin("COMPLETED", "SHIPPED"))
    .withColumn("order_year", F.year("order_date"))
    .withColumn("order_month", F.date_format("order_date", "MM"))
    .select("order_id", "customer_id", "product_id", "order_date", "quantity",
            "unit_price", "gross_amount", "status", "channel", "is_revenue",
            "_ingest_ts", "_source_file", "order_year", "order_month")
)
write_silver(fact_orders, "fact_orders", ["order_year", "order_month"])

reject_reason = F.concat_ws("; ",
    F.when(F.col("customer_id").isNull(), F.lit("missing_customer_id")),
    F.when(F.col("product_id").isNull(), F.lit("missing_product_id")),
    F.when(F.col("order_date").isNull(), F.lit("unparseable_order_date")),
    F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
    F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("invalid_unit_price")),
)

quarantine = (
    deduped.filter(~is_valid)
    .withColumn("reject_reason", reject_reason)
    .withColumn("quarantined_at", F.current_timestamp())
)
write_silver(quarantine, "quarantine_orders", [])

job.commit()
