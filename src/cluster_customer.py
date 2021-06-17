import pyspark
import pandas as pd
import pyspark.sql.functions as f
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler

from data_preparation import DataPreparation


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
            sdf_customer_profile = self.data.spark.read.csv("src/data/customer_profile_new.csv", header=True,inferSchema=True)
            sdf_customer_profile.printSchema()

        sdf_customer_profile = sdf_customer_profile.where(sdf_customer_profile["avg_turnover_per_session"] > 0)
        vectorized_data = self.vectorize(sdf_customer_profile)
        standardized_data = self.scale(vectorized_data)

        (trainingData, testData, devData) = standardized_data.randomSplit([0.6, 0.3, 0.01], seed=123)

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

    def scale(self, v_data):
        self.log.info("Start scaling")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaled_data = scaler.fit(v_data)
        scaled_data_ouptut = scaled_data.transform(v_data)

        # scaled_data_ouptut.select("features", "scaled_features").show(truncate=False)
        return scaled_data_ouptut

    def k_means(self, trainData, testData, k=4):
        # v_trainData = self.vectorize(trainData)
        # v_testData = self.vectorize(testData)

        self.log.info("Trains a k-means model.")
        kmeans = KMeans(featuresCol="scaled_features", k=k, seed=123)
        model = kmeans.fit(trainData)

        self.log.info("Make predictions")
        predictions = model.transform(testData)

        # Evaluate clustering by computing Silhouette score
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        self.log.debug(f"Silhouette with squared euclidean distance = {str(silhouette)}\n")

        self.log.debug("Evaluate clustering.")
        cost = model.summary.trainingCost
        self.log.debug("Within Set Sum of Squared Errors = " + str(cost))

        # # Shows the result.
        self.log.debug("Cluster Centers: ")
        # ctr = []
        centers = model.clusterCenters()
        # for center in centers:
        #     ctr.append(center)
        #     # print(center)
        # self.log.debug(ctr)

        # self.log.info("Save Model")
        # model.write().overwrite().save("src/data/models/kmeans")

        # self.visualize(model, predictions)

        return model, silhouette, centers, predictions, cost

    def evaluate(self, trainData, testData):
        k_values = []
        cost_values = []
        for k in range(2, 50, 2):
            results = self.k_means(trainData, testData, k)
            k_values.append(k)
            cost_values.append(results[4])

        # Plot the cost
        df_cost = pd.DataFrame(cost_values, columns=["cost"])
        df_cost.insert(1, 'k', k_values)

        fig = px.line(df_cost, x="k", y="cost", title="Elbow Curve")

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

        fig2 = px.scatter(predictions.toPandas(), x="avg_events_per_session", y="avg_turnover_per_session", color="predicted_group", hover_data=["user_id", "sum_turnover", "count_session"], title="K-Means: Visualize Clustering in 2D")

        fig2.show()
        fig.show()


if __name__ == "__main__":
    customer = ClusterCustomer()
    train, test, dev = customer.prep_data(True)
    # result = customer.evaluate(train, test)
    result = customer.k_means(train, test)
    # print(result)
    pass
