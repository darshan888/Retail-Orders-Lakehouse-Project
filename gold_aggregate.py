import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "target_bucket"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["target_bucket"]


def write_gold(df, table):
    path = f"s3://{BUCKET}/gold/{table}/"
    glueContext.purge_s3_path(path, {"retentionPeriod": 0})
    sink = glueContext.getSink(
        path=path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx=f"gold_{table}",
    )
    sink.setCatalogInfo(catalogDatabase="retail_gold", catalogTableName=table)
    sink.setFormat("glueparquet")
    sink.writeFrame(DynamicFrame.fromDF(df, glueContext, table))
    print(f"[gold] {table}: wrote {df.count()} rows")


fact = spark.table("retail_silver.fact_orders")
dim_c = spark.table("retail_silver.dim_customers")
dim_p = spark.table("retail_silver.dim_products")

# Gold recognizes revenue only for fulfilled orders.
revenue = fact.filter(F.col("is_revenue"))

enriched = (
    revenue.join(F.broadcast(dim_p), "product_id", "left")
           .join(F.broadcast(dim_c), "customer_id", "left")
           .withColumn("cost_amount",
                       (F.col("quantity") * F.col("cost_price")).cast("decimal(14,2)"))
           .withColumn("margin_amount",
                       (F.col("gross_amount") - F.col("quantity") * F.col("cost_price"))
                       .cast("decimal(14,2)"))
)
enriched.cache()

# 1) Daily sales by category
write_gold(
    enriched.groupBy("order_date", "category", "subcategory").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum("gross_amount").cast("decimal(16,2)").alias("revenue"),
        F.sum("cost_amount").cast("decimal(16,2)").alias("cost"),
        F.sum("margin_amount").cast("decimal(16,2)").alias("margin"),
    ).orderBy("order_date", "category"),
    "agg_daily_sales_by_category",
)

# 2) Monthly revenue by country and segment
write_gold(
    enriched.groupBy("order_year", "order_month", "country", "segment").agg(
        F.countDistinct("order_id").alias("order_count"),
        F.countDistinct("customer_id").alias("active_customers"),
        F.sum("gross_amount").cast("decimal(16,2)").alias("revenue"),
        (F.sum("gross_amount") / F.countDistinct("order_id"))
            .cast("decimal(14,2)").alias("avg_order_value"),
    ).orderBy("order_year", "order_month", "country"),
    "agg_monthly_revenue_by_country",
)

# 3) Customer 360
max_date = enriched.agg(F.max("order_date")).collect()[0][0]
write_gold(
    enriched.groupBy("customer_id", "customer_name", "country", "city", "segment").agg(
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("order_id").alias("lifetime_orders"),
        F.sum("gross_amount").cast("decimal(16,2)").alias("lifetime_revenue"),
        F.sum("margin_amount").cast("decimal(16,2)").alias("lifetime_margin"),
        F.countDistinct("category").alias("distinct_categories"),
    )
    .withColumn("avg_order_value",
                (F.col("lifetime_revenue") / F.col("lifetime_orders")).cast("decimal(14,2)"))
    .withColumn("recency_days", F.datediff(F.lit(max_date), F.col("last_order_date")))
    .withColumn("value_tier",
                F.when(F.col("lifetime_revenue") >= 2000, "HIGH")
                 .when(F.col("lifetime_revenue") >= 500, "MEDIUM")
                 .otherwise("LOW"))
    .orderBy(F.col("lifetime_revenue").desc()),
    "customer_360",
)

# 4) Product performance ranking
write_gold(
    enriched.groupBy("product_id", "product_name", "category").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("gross_amount").cast("decimal(16,2)").alias("revenue"),
        F.sum("margin_amount").cast("decimal(16,2)").alias("margin"),
        F.countDistinct("customer_id").alias("unique_buyers"),
    )
    .withColumn("margin_pct",
                (F.col("margin") / F.col("revenue") * 100).cast("decimal(6,2)"))
    .orderBy(F.col("revenue").desc()),
    "top_products",
)

job.commit()
