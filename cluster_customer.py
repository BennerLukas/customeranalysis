import os
import pyspark
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as T
import plotly.express as px
import plotly.graph_objects as go

from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.feature import VectorAssembler

class DataPreparation:

    def __init__(self) -> None:
        self.spark, self.sc = self.create_spark_session()
    
    def create_spark_session(self):
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
        sdf_session = sdf.select("user_id", "user_session", "event_type", "product_id", "price", "event_time",'purchases','views', 'carts') 

        sdf_session = sdf_session.withColumn("bought_product", f.when(sdf_session.event_type == "purchase", sdf_session["product_id"]).otherwise(None))
        sdf_session = sdf_session.withColumn("first_event", sdf_session.event_time)
        sdf_session = sdf_session.withColumn("last_event", sdf_session.event_time)

        sdf_session_agg = sdf_session.groupBy("user_id", "user_session").agg(f.avg("price"), f.sum("views"), f.sum("purchases"), f.sum("carts"), f.min("event_time"), f.max("event_time"), f.collect_list("bought_product"))
        
        sdf_session_agg = sdf_session_agg.withColumn("duration", (sdf_session_agg["max(event_time)"] - sdf_session_agg["min(event_time)"]))
        sdf_session_agg = sdf_session_agg.withColumn("sum(events)", (sdf_session_agg["sum(views)"] + sdf_session_agg["sum(purchases)"] + sdf_session_agg["sum(carts)"]))
        sdf_session_agg = sdf_session_agg.withColumn("turnover", f.when(sdf_session_agg["sum(purchases)"] > 0, (sdf_session_agg["sum(purchases)"] *  sdf_session_agg["avg(price)"])).otherwise(0))
        sdf_session_agg = sdf_session_agg.withColumn("avg(price)", f.round(sdf_session_agg["avg(price)"],2) )
        sdf_session_agg = sdf_session_agg.withColumn("successfull", f.when(sdf_session_agg["sum(purchases)"] > 0, 1).otherwise(0))
        
        sdf_session_agg.printSchema()
        return sdf
    
    def make_customer_profiles(self, sdf_session_agg):
        sdf_customer_profile = sdf_session_agg.groupBy("user_id").agg(f.sum("sum(events)").alias("sum_events"), f.sum("sum(views)").alias("sum_views"), f.sum("sum(purchases)").alias("sum_purchases"), f.sum("sum(carts)").alias("sum_carts"), f.sum("turnover").alias("sum_turnover"), f.count("user_session").alias("count_session"), f.sum("successfull").alias("sum_successfull"), f.collect_list("collect_list(bought_product)").alias("bought_product"), f.collect_list("user_session").alias("user_sessions"), f.avg("duration"))

        sdf_customer_profile = sdf_customer_profile.withColumn("avg_turnover_per_session", (sdf_customer_profile["sum_turnover"] / sdf_customer_profile["count_session"]))
        sdf_customer_profile = sdf_customer_profile.withColumn("avg_events_per_session", (sdf_customer_profile["sum_events"] / sdf_customer_profile["count_session"]))


        sdf_customer_profile.printSchema()
        return sdf_customer_profile
    
    def export_to_csv(self, sdf, path="data/customer_profile.csv"):
        sdf_export = sdf.withColumn("bought_product", sdf["bought_product"].cast(pyspark.sql.types.StringType()))
        sdf_export = sdf_export.withColumn("user_sessions", sdf_export["user_sessions"].cast(pyspark.sql.types.StringType()))

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

    def prep_data(self):
        sdf = self.data.read_standard_data()
        sdf = self.data.prep_data(sdf)
        sdf_session_agg = self.data.make_session_profiles(sdf)
        sdf_customer_profile = self.data.make_customer_profiles(sdf_session_agg)

        (trainingData, testData, devData) = sdf_customer_profile.where(sdf_customer_profile["avg_turnover_per_session"] > 0).randomSplit([0.6, 0.3, 0.01], seed=123)
        
        return trainingData, testData, devData

    def vectorize(dataset, features=None):
        if features is None:
            features=("sum_views", "sum_turnover", "sum_purchases", "sum_carts", "count_session")

        assembler = VectorAssembler(inputCols=features,outputCol="features")

        dataset=assembler.transform(dataset)
        # dataset.select("features").show(truncate=False)
        return dataset

    def k_means(self, trainData, testData):
        v_trainData = self.vectorize(trainData)
        v_testData = self.vectorize(testData)

        print("Trains a k-means model.")
        kmeans = KMeans().setK(4).setSeed(123)
        model = kmeans.fit(v_trainData)
        
        print("Make predictions")
        predictions = model.transform(v_testData)
        
        # Evaluate clustering by computing Silhouette score
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        print(f"Silhouette with squared euclidean distance = {str(silhouette)}\n")
        
        
        # Evaluate clustering.
        # cost = model.computeCost(dataset)
        # print("Within Set Sum of Squared Errors = " + str(cost))
        
        # Shows the result.
        print("Cluster Centers: ")
        ctr=[]
        centers = model.clusterCenters()
        for center in centers:
            ctr.append(center)
            print(center)
        
        print("Save Model")
        model.write().overwrite().save("knn-model")

        return model, silhouette, centers

    def gaussian_mixture(self, trainData, testData):
        pass

    def visualize(self):
        pass

