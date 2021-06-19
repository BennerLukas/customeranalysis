# General Pyspark Implementation
import pyspark
import pyspark.sql.functions as f

# Machine Learning libary in Pyspark
import pyspark.ml as ml

from data_preparation import DataPreparation


class FrequencyPatternMining:

    def __init__(self) -> None:
        self.aggregated_dataset = None
        self.data = DataPreparation()
        self.log = self.data.set_logger("FPGrowth")
        self.log.info("Finished Init")

    # def save_reopen_data(self, dataset, path="data/fpm.csv"):
    #     dataset.coalesce(1).write.format("csv").mode("overwrite").save(path, header="true")
    #     sdf = self.data.spark.read.csv(path, header=True, inferSchema=True)
    #
    #     self.log.debug("Reopened dataset")
    #
    #     return sdf

    def shape_data(self, dataset, group_by_column="user_id", filter_element=None, focus="product_id"):
        self.log.info("Aggregating dataset")

        if filter_element is None:
            self.aggregated_dataset = dataset.select(group_by_column, focus).distinct() \
                .groupBy(group_by_column) \
                .agg(f.collect_list(focus)).withColumnRenamed(f'collect_list({focus})', 'items')

        else:
            self.aggregated_dataset = dataset.where(dataset[filter_element] == 1).select(group_by_column,
                                                                                         focus).distinct() \
                .groupBy(group_by_column) \
                .agg(f.collect_list(focus)).withColumnRenamed(f'collect_list({focus})', 'items')

        # self.aggregated_dataset = self.save_reopen_data(self.aggregated_dataset)
        return self.aggregated_dataset

    def prep_data(self, small_dataset=False):
        sdf = self.data.read_standard_data(small_dataset)
        sdf = self.data.add_feature_engineering(sdf)

        self.log.info("Finished base data load")

        return sdf

    def train(self, data, min_support=0.001, min_confidence=0.7, save=False):
        self.log.info("Trains FPGrowth model.")
        model = ml.fpm.FPGrowth(minSupport=min_support, minConfidence=min_confidence)
        model = model.fit(data)

        self.log.info("Make predictions")
        model.setPredictionCol("newPrediction")

        # # Shows itemsets
        self.log.debug("Frequent Items:")
        self.log.debug(model.freqItemsets.show(5))

        # # Shows found rules
        self.log.debug("Found rules:")
        self.log.debug(model.associationRules.show(truncate=False))

        if save:
            self.log.info("Saving Model")
            model.write().overwrite().save(f"data/models/fpgrowth")

        return model


if __name__ == "__main__":
    fpm = FrequencyPatternMining()
    sdf = fpm.prep_data(small_dataset=True)
    sdf = fpm.shape_data(sdf, filter_element="views")
    model = fpm.train(sdf, min_support=0.001)
    pass
