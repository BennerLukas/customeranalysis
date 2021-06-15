import logging
import pyspark
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as T
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.feature import VectorAssembler


class DataPreparation:

    def __init__(self) -> None:
        self.log = self.set_logger("Data-Prep")
        self.spark, self.sc = self.create_spark_session()

    def set_logger(self, name, mode=logging.INFO):
        log_format = ('[DataExploration][%(asctime)s] [%(levelname)-8s][%(name)-12s] %(message)s')

        logging.basicConfig(
            level=mode,
            format=log_format,
            filename=('src/data/log/debug.log'),
        )

        logging.debug('debug')
        logging.info('info')
        logging.warning('warning')
        logging.error('error')
        logging.critical('critical')

        logger = logging.getLogger(name)
        logFormatter = logging.Formatter("%(asctime)s [%(levelname)-8s] [%(name)-12s]  %(message)s")

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

        sdf = sdf.withColumn("category_class", f.split(sdf["category_code"], "\.").getItem(0))
        sdf = sdf.withColumn("category_sub_class", f.split(sdf["category_code"], "\.").getItem(1))
        sdf = sdf.withColumn("category_sub_sub_class", f.split(sdf["category_code"], "\.").getItem(2))

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
                sdf_session_agg["max(event_time)"] - sdf_session_agg["min(event_time)"]))
        sdf_session_agg = sdf_session_agg.withColumn("sum(events)", (
                sdf_session_agg["sum(views)"] + sdf_session_agg["sum(purchases)"] + sdf_session_agg["sum(carts)"]))
        sdf_session_agg = sdf_session_agg.withColumn("turnover", f.when(sdf_session_agg["sum(purchases)"] > 0, (
                sdf_session_agg["sum(purchases)"] * sdf_session_agg["avg(price)"])).otherwise(0))
        sdf_session_agg = sdf_session_agg.withColumn("avg(price)", f.round(sdf_session_agg["avg(price)"], 2))
        sdf_session_agg = sdf_session_agg.withColumn("successfull",
                                                     f.when(sdf_session_agg["sum(purchases)"] > 0, 1).otherwise(0))

        sdf_session_agg.printSchema()
        return sdf

    def make_customer_profiles(self, sdf_session_agg):
        self.log.info("Create Customer Profiles")
        sdf_customer_profile = sdf_session_agg.groupBy("user_id").agg(f.sum("sum(events)").alias("sum_events"),
                                                                      f.sum("sum(views)").alias("sum_views"),
                                                                      f.sum("sum(purchases)").alias("sum_purchases"),
                                                                      f.sum("sum(carts)").alias("sum_carts"),
                                                                      f.sum("turnover").alias("sum_turnover"),
                                                                      f.count("user_session").alias("count_session"),
                                                                      f.sum("successfull").alias("sum_successfull"),
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


class ClusterCustomer:

    def __init__(self) -> None:
        self.data = DataPreparation()
        self.log = self.data.set_logger("K-Means")
        self.log.info("Finished Init")
        self.features = ("avg_turnover_per_session", "avg_events_per_session", "sum_turnover", "count_session")
        # features = ("sum_views", "sum_turnover", "sum_purchases", "sum_carts", "count_session")

    def prep_data(self, read_existing=True):
        if read_existing is False:
            self.log.info("Create customer profile vom scratch")
            sdf = self.data.read_standard_data()
            sdf = self.data.add_feature_engineering(sdf)
            sdf_session_agg = self.data.make_session_profiles(sdf)
            sdf_customer_profile = self.data.make_customer_profiles(sdf_session_agg)
        else:
            self.log.info("Read customer profile from CSV")
            sdf_customer_profile = self.data.spark.read.csv("src/data/customer_profile_new.csv", header=True,
                                                            inferSchema=True)
            sdf_customer_profile.printSchema()

        (trainingData, testData, devData) = sdf_customer_profile.where(
            sdf_customer_profile["avg_turnover_per_session"] > 0).randomSplit([0.6, 0.3, 0.01], seed=123)

        return trainingData, testData, devData

    def vectorize(self, dataset, features=None):
        self.log.info("Start vectorizing")
        if features is None:
            features = self.features
        else:
            self.features = features

        assembler = VectorAssembler(inputCols=features, outputCol="features")

        dataset = assembler.transform(dataset)
        # dataset.select("features").show(truncate=False)
        return dataset

    def k_means(self, trainData, testData, k=4):
        v_trainData = self.vectorize(trainData)
        v_testData = self.vectorize(testData)

        self.log.info("Trains a k-means model.")
        kmeans = KMeans().setK(k).setSeed(123)
        model = kmeans.fit(v_trainData)

        self.log.info("Make predictions")
        model.setPredictionCol("newPrediction")
        predictions = model.predict(v_testData)

        # Evaluate clustering by computing Silhouette score
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        self.log.debug(f"Silhouette with squared euclidean distance = {str(silhouette)}\n")

        self.log.debug("Evaluate clustering.")
        cost = model.summary.trainingCost
        self.log.debug("Within Set Sum of Squared Errors = " + str(cost))

        # Shows the result.
        self.log.debug("Cluster Centers: ")
        ctr = []
        centers = model.clusterCenters()
        for center in centers:
            ctr.append(center)
            # print(center)
        self.log.debug(ctr)

        self.log.info("Save Model")
        model.write().overwrite().save("src/data/models/kmeans")

        # self.visualize(model, predictions)

        return model, silhouette, centers, predictions, cost

    def evaluate(self, trainData, testData):
        cost = np.zeros(10)
        for k in range(2, 10):
            results = self.k_means(trainData, testData, k)
            cost[k] = results[4]

        # Plot the cost
        df_cost = pd.DataFrame(cost[2:])
        df_cost.columns = ["cost"]
        new_col = [2, 3, 4, 5, 6, 7, 8, 9]
        df_cost.insert(0, 'k', new_col)

        fig = px.line(df_cost.k, df_cost.cost, title="Elbow Curve", labels={"k": "Number of K",
                                                                            "cost": "Cost"})

        fig.show()
        return df_cost

    def gaussian_mixture(self, trainData, testData):
        pass

    def visualize(self, model, predictions):
        predictions.show()

        centers = pd.DataFrame(model.clusterCenters(), columns=self.features)
        print(centers.head())

        fig = px.scatter(centers, x="avg_events_per_session", y="avg_turnover_per_session")
        predictions = predictions.withColumn("predicted_group", predictions["prediction"].cast(pyspark.sql.types.StringType()))

        fig2 = px.scatter(predictions.toPandas(), x="avg_events_per_session", y="avg_turnover_per_session", color="predicted_group")

        fig2.show()
        fig.show()


if __name__ == "__main__":
    customer = ClusterCustomer()
    train, test, dev = customer.prep_data(True)
    result = customer.evaluate(train, test)
    # result = customer.k_means(train, test)
    # print(result)
    pass
