# General Pyspark Implementation
import pyspark
import pyspark.sql.functions as f

# Machine Learning libary in Pyspark
import pyspark.ml as ml

# from ../data_preparation import DataPreparation
from src.modelling.data_preparation import DataPreparation


class FrequencyPatternMining:

    def __init__(self) -> None:
        """
        Initialisiert eine Instanz der DataPrepartion Klasse und erstellt einen Logger.
        """
        self.aggregated_dataset = None
        self.data = DataPreparation()
        self.log = self.data.set_logger("FPGrowth")
        self.log.info("Finished Init")

    def shape_data(self, dataset, group_by_column="user_id", filter_element=None, focus="product_id"):
        """
        Aggregiert die Daten über einen Groupby über die group_by_column um ein für das FPM notwendiges Datenformat zu
        erhalten.

        :param dataset: Spark Dataframe mit mindestens den Spalten mit den Name group_by_column und focus
        :param group_by_column: Spalte nach der das Dataframe gruppiert wird
        :param filter_element: optionaler Param um zwischen Purchase/Views/Carts zu wechseln und zu spezifizieren
        :param focus: Spalte, die später analysiert werden soll (bspw. "product_id" oder "category_code")
        :return: Aggregiertes Spark Dataframe
        """
        self.log.info("Aggregating dataset")
        self.focus = focus
        if filter_element is None:
            self.aggregated_dataset = dataset.select(group_by_column, focus) \
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
        """
        Liest die Daten ein und fügt Features hinzu.

        :param small_dataset: Um nur Daten aus November zu nutzen
        :return: Spark Dataframe
        """
        sdf = self.data.read_standard_data(small_dataset)
        sdf = self.data.add_feature_engineering(sdf)

        self.log.info("Finished base data load")

        return sdf

    def train(self, data, min_support=0.001, min_confidence=0.7, save=False):
        """
        Trainert ein FPGrowth-Modell mit den gegebenen Parametern und speichert dieses dann ggf. im Anschluss.

        :param data: Spark Dataframe mit Array-Spalte mit Name "items"
        :param min_support: minimal benötigte Support um eine Assoziationsregel anzunehmen
        :param min_confidence: minimal benötigte Konfidenz um eine Assoziationsregel anzunehmen
        :param save: Startet den Speichervorgang fürs Modell
        :return:
        """
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
            model.write().overwrite().save(f"data/models/fpgrowth_{self.focus}")

        return model


if __name__ == "__main__":
    #Erstelle Klasse in der FPM betrieben wird
    fpm = FrequencyPatternMining()
    #Hole Datensatz
    sdf = fpm.prep_data(small_dataset=True)
    #Aggrregiere Datensatz in Spalte mit Arrays
    sdf = fpm.shape_data(sdf, filter_element="purchases", focus="category_sub_sub_class")
    model = fpm.train(sdf, min_support=0.0001, min_confidence=0.7)
    pass
