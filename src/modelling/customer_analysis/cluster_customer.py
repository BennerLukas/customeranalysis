import pyspark
import pandas as pd
import pyspark.sql.functions as f
import plotly.express as px
import plotly.graph_objects as go
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, MaxAbsScaler
from pyspark.ml.functions import vector_to_array

# from data_preparation import DataPreparation
from src.modelling.data_preparation import DataPreparation


class ClusterCustomer:

    def __init__(self) -> None:
        self.data = DataPreparation()
        self.log = self.data.set_logger("K-Means")
        self.log.info("Finished Init")
        self.features = ("avg_turnover_per_session", "avg_events_per_session", "sum_turnover", "count_session",
                         "sum_views", "sum_purchases", "sum_carts", "avg(duration)")

    def prep_data(self, read_existing=True):
        if read_existing is False:
            self.log.info("Create customer profile vom scratch")
            sdf = self.data.read_standard_data()
            sdf = self.data.add_feature_engineering(sdf)
            sdf_session_agg = self.data.make_session_profiles(sdf)
            sdf_customer_profile = self.data.make_customer_profiles(sdf_session_agg)
            self.data.export_to_csv(sdf_customer_profile, "data/customer_profile.csv")
        else:
            self.log.info("Read customer profile from CSV")
            sdf_customer_profile = self.data.spark.read.csv("../../data/customer_profile.csv", header=True,
                                                            inferSchema=True)
            # sdf_customer_profile.printSchema()

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
        scaler = MaxAbsScaler(inputCol="features", outputCol="scaled_features")
        scaled_data = scaler.fit(v_data)
        scaled_data_ouptut = scaled_data.transform(v_data)

        # scaled_data_ouptut.select("features", "scaled_features").show(truncate=False)
        return scaled_data_ouptut

    def k_means(self, trainData, testData, k=10):
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

        self.visualize(model, predictions)

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

    def visualize(self, model, predictions):
        self.log.info("Visualize result")
        # predictions.show()
        predictions = predictions.withColumn("predicted_group",
                                             predictions["prediction"].cast(pyspark.sql.types.StringType()))

        fig2 = px.scatter(predictions.toPandas(), x="avg_events_per_session", y="avg_turnover_per_session",
                          color="predicted_group", hover_data=["user_id", "sum_turnover", "count_session"],
                          title="K-Means: Visualize Clustering in 2D")
        fig2.show()
        n = len(self.features)
        scaled_array = predictions.select("prediction", vector_to_array("scaled_features"))
        result = scaled_array.select("prediction", *[scaled_array["UDF(scaled_features)"][i] for i in range(n)])
        # result.show()

        avg_per_feature = result.groupBy("prediction").agg(
            f.avg("UDF(scaled_features)[0]").alias("avg_turnover_per_session"),
            f.avg("UDF(scaled_features)[1]").alias("avg_events_per_session"),
            f.avg("UDF(scaled_features)[2]").alias("sum_turnover"),
            f.avg("UDF(scaled_features)[3]").alias("count_session"),
            f.avg("UDF(scaled_features)[4]").alias("sum_views"),
            f.avg("UDF(scaled_features)[5]").alias("sum_purchases"),
            f.avg("UDF(scaled_features)[6]").alias("sum_carts"),
            f.avg("UDF(scaled_features)[7]").alias("avg(duration)"),
            f.stddev("UDF(scaled_features)[0]").alias("dev_avg_turnover_per_session"),
            f.stddev("UDF(scaled_features)[1]").alias("dev_avg_events_per_session"),
            f.stddev("UDF(scaled_features)[2]").alias("dev_sum_turnover"),
            f.stddev("UDF(scaled_features)[3]").alias("dev_count_session"),
            f.stddev("UDF(scaled_features)[4]").alias("dev_sum_views"),
            f.stddev("UDF(scaled_features)[5]").alias("dev_sum_purchases"),
            f.stddev("UDF(scaled_features)[6]").alias("dev_sum_carts"),
            f.stddev("UDF(scaled_features)[7]").alias("dev_avg(duration)")
            )

        # avg_per_feature.show()

        df_avg_per_feature = avg_per_feature.toPandas()

        fig = go.Figure()
        for feature in self.features:
            fig.add_trace(go.Bar(
                x=df_avg_per_feature.prediction,
                y=df_avg_per_feature[feature],
                error_y=dict(type='data', array=df_avg_per_feature[f"dev_{feature}"]),
                name=feature,
            ))

        fig.update_layout(barmode='group', title="Scaled Feature per Group", legend_title="Feature")
        fig.update_xaxes(type='category')
        fig.show()


if __name__ == "__main__":
    customer = ClusterCustomer()
    train, test, dev = customer.prep_data(True)
    # result = customer.evaluate(train, test)
    result = customer.k_means(train, test)
    # print(result)
