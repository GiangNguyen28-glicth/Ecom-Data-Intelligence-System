from pyspark.sql import Window
from pyspark.sql.functions import count, sum, col, broadcast, row_number, dense_rank, to_date

from adapters.minio_spark_adapter import minio_spark_adapter
from schema.ecm_olist import orders_schema, order_items_schema, products_schema, sellers_schema, customers_schema


class ECMOlist:
    def __init__(self):
        print('Initializing ECMOlist')
        self.spark = minio_spark_adapter.create_spark_session()
        self.olist_orders_dataset = "olist_orders_dataset"
        self.olist_order_items_dataset = "olist_order_items_dataset"
        self.olist_products_dataset = "olist_products_dataset"
        self.olist_sellers_dataset = "olist_sellers_dataset"
        self.olist_customers_dataset = "olist_customers_dataset"

    def load_df(self, dataset_name, schema):
        dataset_path = f"s3a://kaggle-data/{dataset_name}.csv"
        return self.spark.read.schema(schema).csv(dataset_path, header=True)

    def orders_per_status(self):
        orders_df = self.load_df("olist_orders_dataset", orders_schema)
        orders_df.groupby("order_status").agg(count("*").alias("total_orders")).show(10)

    def total_revenue(self):
        orders_df = self.load_df(self.olist_orders_dataset, orders_schema)
        orders_df.agg(sum("*").alias("total_revenue")).show()

    def top_10_products(self):
        orders_df = self.load_df(self.olist_order_items_dataset, order_items_schema)
        orders_df.groupby("product_id").agg(count("*").alias("total_sold")).orderBy("total_sold", ascending=False).show(
            10)

    def revenue_per_product_category(self):
        oi = self.load_df(self.olist_order_items_dataset, order_items_schema).alias("oi")
        p = self.load_df(self.olist_products_dataset, products_schema).alias("p")
        oi.join(broadcast(p),
                "product_id").groupby(col("p.product_category_name")).agg(
            sum("price").alias("total_revenue")).show(10)

    def top_sellers(self):
        s = self.load_df(self.olist_sellers_dataset, sellers_schema).alias("s")
        oi = self.load_df(self.olist_order_items_dataset, order_items_schema).alias("oi")
        oi.join(broadcast(s),"seller_id").groupby(col("s.seller_id")).agg(sum("oi.price").alias("total_revenue")).orderBy("total_revenue", ascending=False).show(10)

    def orders_per_city(self):
        o = self.load_df(self.olist_orders_dataset, orders_schema).alias("o")
        c = self.load_df(self.olist_customers_dataset, customers_schema).alias("c")
        o.join(broadcast(c), "customer_id").groupby(col("c.customer_city")).agg(count("*").alias("total_orders")).orderBy("total_orders", ascending=False).show(10)

    def top_products_by_category(self):
        window_spec = Window.partitionBy("product_category_name").orderBy(col("total_sold").desc())
        p = self.load_df(self.olist_products_dataset, products_schema).alias("p")
        oi = self.load_df(self.olist_order_items_dataset, order_items_schema).alias("oi")
        df = oi.join(broadcast(p), "product_id").groupby("p.product_id", "p.product_category_name").agg(count("*").alias("total_sold"))
        wd_df = df.withColumn("rank", row_number().over(window_spec))
        wd_df.filter(
            col("product_category_name").isNotNull()
        ).show(10)

    def customer_ranking(self):
        o = self.load_df(self.olist_orders_dataset, orders_schema).alias("o")
        oi = self.load_df(self.olist_order_items_dataset, order_items_schema).alias("oi")
        df = o.join(oi,"order_id").groupby(col("o.customer_id")).agg(sum(col("oi.price").cast("double")).alias("total_revenue"))
        window_spec = Window.orderBy(col("total_revenue").desc())
        df = df.withColumn("rank", dense_rank().over(window_spec)).show(10)

    def seller_daily_revenue(self):
        oi = self.load_df(self.olist_order_items_dataset, order_items_schema).alias("oi")
        o = self.load_df(self.olist_orders_dataset, orders_schema).alias("o")
        df = oi.join(o, "order_id").filter(col("order_status") == "delivered").withColumn("order_date", to_date("order_purchase_timestamp"))
        window_spec = Window.partitionBy("seller_id", "order_date")
        df.withColumn("revenue", sum("price").over(window_spec)).select("revenue", "order_date", "seller_id").dropDuplicates().show(10)
