import logging
import pyspark
import pyspark.sql.functions as f


class DataPreparation:

    def __init__(self) -> None:
        self.log = self.set_logger("Data-Prep")
        self.spark, self.sc = self.create_spark_session()

    @staticmethod
    def set_logger(name, mode=logging.INFO):
        log_format = '[DataExplo][%(asctime)s][%(threadName)-12.12s][%(levelname)-8s][%(name)-12s] %(message)s'

        logging.basicConfig(
            level=mode,
            format=log_format,
            filename='data/log/debug.log',
        )

        logging.debug('debug')
        logging.info('info')
        logging.warning('warning')
        logging.error('error')
        logging.critical('critical')

        logger = logging.getLogger(name)
        logFormatter = logging.Formatter(log_format)

        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        logger.addHandler(consoleHandler)

        return logger

    def create_spark_session(self):
        self.log.info("Create Spark Session")
        # spark = pyspark.sql.SparkSession.builder.appName("app1").getOrCreate()
        spark = pyspark.sql.SparkSession \
            .builder \
            .master("local") \
            .appName("app_great") \
            .config("spark.executor.memory", f"16g") \
            .config("spark.driver.memory", f"16g") \
            .config("spark.memory.offHeap.enabled", True) \
            .config("spark.memory.offHeap.size", f"16g") \
            .config("spark.sql.debug.maxToStringFields", f"16") \
            .getOrCreate()
        sc = spark.sparkContext
        return spark, sc

    def add_feature_engineering(self, sdf):
        self.log.info("Start feature engineering")
        # Feature Splitting
        # sdf = sdf.withColumn("category_class", f.substring_index(sdf.category_code, '.', 1))

        sdf = sdf.withColumn("category_class", f.split(sdf["category_code"], r"\.").getItem(0))
        sdf = sdf.withColumn("category_sub_class", f.split(sdf["category_code"], r"\.").getItem(1))
        sdf = sdf.withColumn("category_sub_sub_class", f.split(sdf["category_code"], r"\.").getItem(2))

        sdf = sdf.withColumn("year", f.year("event_time"))
        sdf = sdf.withColumn("month", f.month("event_time"))
        sdf = sdf.withColumn("weekofyear", f.weekofyear("event_time"))
        sdf = sdf.withColumn("dayofyear", f.dayofyear("event_time"))
        sdf = sdf.withColumn("dayofweek", f.dayofweek("event_time"))
        sdf = sdf.withColumn("dayofmonth", f.dayofmonth("event_time"))
        sdf = sdf.withColumn("hour", f.hour("event_time"))

        sdf = sdf.withColumn('turnover', f.when(f.col('event_type') == 'purchase', f.col('price')).otherwise(0))

        sdf = sdf.withColumn('purchases', f.when(f.col('event_type') == 'purchase', f.lit(1)).otherwise(0))
        sdf = sdf.withColumn('views', f.when(f.col('event_type') == 'view', f.lit(1)).otherwise(0))
        sdf = sdf.withColumn('carts', f.when(f.col('event_type') == 'cart', f.lit(1)).otherwise(0))

        # None Handling
        # sdf = sdf.fillna(value="not defined")

        # drop extreme outlier around 15th of november -> only works for year 2019
        sdf = sdf.where(sdf["dayofyear"] != 319)
        sdf = sdf.where(sdf["dayofyear"] != 320)
        sdf = sdf.where(sdf["dayofyear"] != 321)

        sdf = sdf.withColumn("event_time", sdf["event_time"].cast(pyspark.sql.types.TimestampType()))

        sdf.printSchema()
        return sdf

    def make_session_profiles(self, sdf):
        self.log.info("Create Session Profiles")
        sdf_session = sdf.select("user_id", "user_session", "event_type", "product_id", "price", "event_time",
                                 'purchases', 'views', 'carts')

        sdf_session = sdf_session.withColumn("bought_product", f.when(sdf_session.event_type == "purchase",
                                                                      sdf_session["product_id"]).otherwise(None))
        sdf_session = sdf_session.withColumn("first_event", sdf_session.event_time)
        sdf_session = sdf_session.withColumn("last_event", sdf_session.event_time)

        sdf_session_agg = sdf_session.groupBy("user_id", "user_session").agg(f.avg("price"), f.sum("views"),
                                                                             f.sum("purchases"), f.sum("carts"),
                                                                             f.min("event_time"), f.max("event_time"),
                                                                             f.collect_list("bought_product"))

        sdf_session_agg = sdf_session_agg.withColumn("duration", (
                sdf_session_agg["max(event_time)"].cast("long") - sdf_session_agg["min(event_time)"].cast("long")))
        sdf_session_agg = sdf_session_agg.withColumn("sum(events)", (
                sdf_session_agg["sum(views)"] + sdf_session_agg["sum(purchases)"] + sdf_session_agg["sum(carts)"]))
        sdf_session_agg = sdf_session_agg.withColumn("turnover", f.when(sdf_session_agg["sum(purchases)"] > 0, (
                sdf_session_agg["sum(purchases)"] * sdf_session_agg["avg(price)"])).otherwise(0))
        sdf_session_agg = sdf_session_agg.withColumn("avg(price)", f.round(sdf_session_agg["avg(price)"], 2))
        sdf_session_agg = sdf_session_agg.withColumn("successfully",
                                                     f.when(sdf_session_agg["sum(purchases)"] > 0, 1).otherwise(0))

        sdf_session_agg.printSchema()
        sdf_session_agg.show()
        return sdf_session_agg

    def make_customer_profiles(self, sdf_session_agg):
        self.log.info("Create Customer Profiles")
        sdf_session_agg.show()
        sdf_customer_profile = sdf_session_agg.groupBy("user_id").agg(f.sum("sum(events)").alias("sum_events"),
                                                                      f.sum("sum(views)").alias("sum_views"),
                                                                      f.sum("sum(purchases)").alias("sum_purchases"),
                                                                      f.sum("sum(carts)").alias("sum_carts"),
                                                                      f.sum("turnover").alias("sum_turnover"),
                                                                      f.count("user_session").alias("count_session"),
                                                                      f.sum("successfully").alias("sum_successfully"),
                                                                      f.collect_list(
                                                                          "collect_list(bought_product)").alias(
                                                                          "bought_product"),
                                                                      f.collect_list("user_session").alias(
                                                                          "user_sessions"), f.avg("duration"))

        sdf_customer_profile = sdf_customer_profile.withColumn("avg_turnover_per_session", (
                sdf_customer_profile["sum_turnover"] / sdf_customer_profile["count_session"]))
        sdf_customer_profile = sdf_customer_profile.withColumn("avg_events_per_session", (
                sdf_customer_profile["sum_events"] / sdf_customer_profile["count_session"]))

        sdf_customer_profile.printSchema()
        return sdf_customer_profile

    def export_to_csv(self, sdf, path="data/customer_profile.csv"):
        self.log.info(f"Export data to {path}")
        sdf_export = sdf.withColumn("bought_product", sdf["bought_product"].cast(pyspark.sql.types.StringType()))
        sdf_export = sdf_export.withColumn("user_sessions",
                                           sdf_export["user_sessions"].cast(pyspark.sql.types.StringType()))

        sdf_export.printSchema()
        sdf_export.coalesce(1).write.format("csv").mode("overwrite").save(path, header="true")
        return True

    def read_standard_data(self):
        sdf_201911 = self.spark.read.csv("data/2019-Nov.csv", header=True, inferSchema=True)
        sdf_201910 = self.spark.read.csv("data/2019-Oct.csv", header=True, inferSchema=True)

        sdf = sdf_201910.union(sdf_201911)
        return sdf


if __name__ == "__main__":
    data_prep = DataPreparation()
    data_prep.log.info("Create customer profile vom scratch")
    sdf = data_prep.read_standard_data()
    sdf = data_prep.add_feature_engineering(sdf)
    sdf_session_agg = data_prep.make_session_profiles(sdf)
    sdf_customer_profile = data_prep.make_customer_profiles(sdf_session_agg)
    data_prep.export_to_csv(sdf_customer_profile)
