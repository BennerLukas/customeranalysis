# Documentation für eShopper



# Einführung 
(Alina)
## Datensatz 
## Tools 


# Daten Exploration 
(Alina)
## ursprüngliche Daten 
## Feature Engineering 
## erhaltenes Wissen durch Data Mining 

# Modellierung

## Kaufverhalten 
(Phillip)

## Kunden-Cluster mit K-Means 
(Lukas)
 ### Datenvorbereitung
Um die Kundenklassifizierung umzusetzen werden die Insights aus der Data Exploration verwendet.
Als erstens werden das Feature-Engineering angewendet. Anschlißend die Daten auf Session-Ebene aggregiert. Mithilfe dieser Daten wird daraufhin ein Kundenprofil für jeden 
Kunden angelegt (siehe ```src/data/data_preparation.py```).

Die Kundenprofile wurden dann in eine eigene Datei ausgelagert. Dadurch kann die Laufzeit deutlich verringert werden.

 ### Training und Programm 
Für die Ermittlung der verschiedenen Kundengruppen wird ein unsupervised Learning Algorithmus benötigt. Hier haben wir uns für k-means entschieden, da dies eine gute 
Erklärbarkeit des Models gewährleistet. Da in diesem Projekt komplett auf Spark gesetzt wurde, wird auch das modelling mit Spark umgesetzt (siehe [K-Means](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.clustering.KMeans.html)).

Das Modelling begann experimentell in einem Notebook und wurde dann in ein Py-File transferiert. Das Py-File bietet bessere Ausführungszeiten und Logging Möglichkeiten.
 ### Evaluieren und Visualisieren 
Beim K-Means Algorithmus gibt es grundsätzlich nur den Parameter k, also die Anzahl der Gruppen (Cluster) die gefunden werden sollen.
Den Seed und die Distanzberechnungsart (Euklid) haben wir konstant gelassen.

Weiterer wichtiger Punkt ist die Auswahl der Features, mit denen das Modell lernen soll. Daher haben wir das Modell mit verschiedenen Kombinationen der Features trainiert.

Für die Ermittlung von k haben wir eine Elbow-Curve aufgestellt, mit der visuell das beste k herausgefunden werden kann
<img src="/src/data/exports/elbow-curve_kmeans.png" alt="elbow" width="500" align="center"/>

 ### Abschließende Interpretationen 
-> DBSCAN?

# Weitere Schritte 
(Alina)