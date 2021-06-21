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

Für die Ermittlung von k haben wir eine Elbow-Curve aufgestellt, mit der visuell das beste k herausgefunden werden kann.
Aus dem Schaubild kann nun ein mögliches ideales k von 10 herausgelesen werden. 
Im Zuge der Erklärbarkeit der Kundengruppen haben wir uns aber zusätzlich dazu entschieden auch einmal ein k=4 auszuprobieren. 
<img src="/src/data/exports/elbow-curve_kmeans.png" alt="elbow" width="800" align="center"/>

Nachdem das Modell mit k=4 trainiert wurde, fällt es schwer festzustellen, ob und wie gut das Modell ist. 
Eine Visualisierung der Cluster wie im Folgenden ist nicht sehr geeignet, da nur 2 Features der ingesamt 8 Features dargestellt werden können:

<img src="https://github.com/BennerLukas/customeranalysis/blob/main/src/data/exports/k-means_2D.png" alt="k=4" width="800" align="center"/>

Bei k=10 wird dies noch unübersichtlicher:

<img src="https://github.com/BennerLukas/customeranalysis/blob/main/src/data/exports/k-means_2D_k10.png" alt="k=10" width="800" align="center"/>

Da man aber ja nicht jeden einzelnen Punkt herausfinden will, sondern es eher interessant ist welche Werte für welche Features man benötigt, um in eine bestimmte Gruppe zu fallen haben wir
folgenden Plot entwickelt.
Dieser stellt den Durchschnittlichen (skalierten) Wert des Features innerhalb dieser einen Gruppe (Cluster) dar (inklusive Standardabweichung).

<iframe src="https://raw.githubusercontent.com/BennerLukas/customeranalysis/main/src/data/exports/k-means_feature_dist_k10_proper_scaled.html"></iframe>

 ### Abschließende Interpretationen 
-> DBSCAN?
-> more features -> better testing
-> identifizeren von custom marketing für bestimmte Gruppen

# Weitere Schritte 
(Alina)