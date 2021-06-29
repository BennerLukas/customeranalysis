# Dokumentation für eShopper



# Einführung 
Das Projekt "customeranalysis" wird von der Gruppe "eShopper" durchgeführt. 
Diese besteht aus den Studenten Alina Buss (4163246), Phillip Lange (...) und Lukas Benner (6550912). 
Hierbei soll das Kundenverhalten innerhalb eines eShops analysiert werden, sodass im Nachhinein Kundensegmente ermittelt und Empfehlungen ausgesprochen werden können.

## Datensatz
Für das Projekt wird auf einen transaktionalen Datensatz von Kaggle zurückgegriffen. Dieser kann unter folgendem Link heruntergeladen werden: https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store. 
Dieser Datensatz umfasst jegliche Events des eShops in den Monaten Oktober und November 2019. Die Events wurden hierbei in die Kategorien "View", "Add to Cart" und "Purchase" unterteilt. Zusätzlich gibt es noch Informationen zu dem Event-Zeitpunkt, der Product-ID, der Category-ID, dem Category-Code, der Marke, dem Preis, der User-Id und der User-Session. 
Hierbei ergibt sich eine Gesamtgröße von ca. 14GB mit 100 Millionen Einträgen. Da die Analysen mit einem so großen Datensatz sehr langsam ausfallen, wurde zusätzlich ein repräsentatives Sample mit nur 100.000 Einträgen erstellt.

## Tools
Zur Analyse des Datensatzes wurden die Pakete "pyspark" und "pandas" genutzt. Zur Visualisierung der Daten wird zusätzlich das Paket "plotly" importiert.

# Daten Exploration 
Im Folgenden werden primär die ursprünglichen Spalten des Datensatzes einzeln analysiert. 
Daraufhin werden auf Basis der neuen Erkenntnissen neue Spalten innerhalb des Feature Enginneerings erstellt und ebenfalls untersucht. 
Daraufhin werden alle Spalten in Abhängigkeit voneinander untersucht, um mögliche Auffälligkeiten zu erkennen. 
Diese Auffälligkeiten sollen mithilfe von Korrelationsmatrizen im letzten Schritt validiert werden.

## Gesonderte Spaltenanalyse
Der ursprüngliche Datensatz besteht aus den Attributen "event_time", "product_id", "category_id", "category_code", "brand", "price", "user_id" and "user_session". 
Hierbei existieren ca. 200.000  Produkte, 4300 Marken, 5.000.000 User und 23.000.000 User-Sessions.
Die Spalten "product_id", "category_id" und "user_id" sind simple IDs die auf das entsprechende Produkt, die Kategorie oder den User hindeuten. 
Die Spalte "user_session" ist ebenfalls eine ID, die die User_Session eindeutig identifiziert. 
Der "category_code" besteht meist aus 2 oder 3 Unterpunkten, die die Kateogrie eines Produktes beschreiben. 
Die beliebtesten category_codes sind hierbei die "electronics.smartphone" und "electronics.clocks". 
Das Attribut "price" ordnet einer Product_ID immer einen eindeutigen Preis zu. 
Die Preise befinden sich hierbei zwischen 0 und 2574€, wobei der Mittlewert bei 292€ liegt. 
Das Attribut "brand" ordnet ebenfalls einem Produkt eine bestimmte Marke zu. Hierbei sind die Marken Samsung, Apple und Xiaomi besonders beliebt. 
Die Spalte "Event_time" besteht aus einem Datum und einem Zeitstempel. Der "event_type" identifiziert um welche Art von Interaktion es sich handelt. 
Hierbei wird unterschieden zwischen den Aktionen "View", "Cart" und "Purchase". Hierbei fällt auf, dass 94,9% der Aktionen Views sind, 3.6% Carts und 1.5% Purchases. 
Dies bedeutet, dass der Datensatz sehr ungleich verteilt ist. 

## Feature Engineering 
Primär wird die Spalte "category_code", die jeweils aus drei Aspekten besteht, in die Kategorien "category_class", "category_sub_class" und "category_sub_sub_class" aufgeteilt. 
Hierbei ergeben sich 13 Haupt-Kategorieklassen. Dabei fällt auf, dass die Klasse "electronics" mit über 50% der Interaktionen die beliebteste Klasse ist. 
Innerhalb dieser Klasse stechen wiederum die Smartphones besonders heraus. 
Daraufhin wird aus der Spalte "event_time", die neuen Attribute "year", "month", "weekofyear", "dayofyear", "dayofmonth", und "hour" extrahiert. 
Diese Attribute sind für folgende Detailanalysen nützlich.
Außerdem konnte ermittelt werden, dass innerhalb einer User_Session eine Product_ID teilweise bis zu 47 Mal gekauft wurde. 
Daraus lässt sich folgern, dass für jedes Exemplar eines Produktes, das gekauft wird, eine neue Transaktion erstellt wird. 
Beruhend auf dieser Erkenntnis konnten zusätzlich die Spalten "Turnover", "bougth_quantity", "viewed_quantity" und "cart_quantity" erstellt werden, die für die folgenden Analysen nützlich sind. 
Hierbei ergibt sich das insgesamt ein Umsatz von ca 50M € generiert wurde. Diese 4 Attribute werden im Folgenden zu den Entscheidungsvariablen, um das Kundenverhalten zu beschreiben.

## Kombinierte Spaltenanalysen 
### Zeitpunkt-Analyse
Primär wird die Abhängigkeit der Entscheidungsvariablen von dem Zeitpunkt analysiert.Hierbei fällt auf, dass die meisten Interaktionen in der Mitte des Monats (15.-17.) stattfinden. 
Dasselbe Verhalten ist auch am Umsatz erkennbar, der ebenfalls in der Mitte des Monats, besonders am 17., am größten ist. 
Hierbei ist der Umsatz am 17. November allerdings ungewöhnlich hoch, während für den 15. November gar kein Umsatz generiert wurde. 
Diese Ausreißer deuten auf einen Systemfehler zu dem entsprechenden Zeitpunkt hin. Nichtsdestotrotz ist eine deutliche Umsatzsteigerung in der Mitte des Monats ableitbar.
Außerdem ist auch erkennbar, dass tendenziell mehr Aktivitäten am Wochenende (Freitag bis Sonntag) stattfinden. Dies wird auch von dem Umsatz bestätigt, der am Sonntag am höchsten ist.
In der Tagesverteilung zeigen sich die Nachmittags-Stunden (15-18Uhr) als besonders interaktionsreich aus, während die Morgen- und Mittagsstunden (6-14Uhr) ein konstant hohes Interaktionsvolumen erreichen. Der Umsatz hingegen hat seinen Peak in der Morgens- und Mittagszeit. Hierbei sticht besonders die Uhrzeit 10 Uhr raus. 

### Produktanalysen
In der Produktanalyse fällt auf, dass ein Category_Class immer mehrere Category_codes umfasst. Diese umfassen wiederum mehrere Category_IDs, welche mehrere Produkt_IDs umfassen. 
Daraus ergibt sich die folgende Beziehung: Product_ID ⊂ Category_ID ⊂ Category_Code ⊂ Category_Class. Die Marken hingegen sind Klassenübergreifend. 
Hierbei fällt ebenfalls aus, dass die Category_Class electronics am meisten Events aller Arten generiert, gefolgt von den appliances und den computers. 
Dieselbe Verteilung lässt sich auch an dem Umsatz erkennen. Gemessen am Umsatz hingegen, rutscht die Marke Apple nun auf den ersten Platz gefolgt von Samsung und Xiaomi. 

### Useranalyse
Innerhalb der Useranalyse fällt auf, dass der Online-Shop von Oktober mit 3,02M User nach November mit 3.696M User deutlich angewachsen ist. 
Hierbei hatten 1,4M User in beiden Monaten eine Interaktion mit dem Shop. Die User haben dabei zwischen 1 und 22.542 User-Sessions, wobei der Mittelwert bei 4.3 liegt. 
Ein User schaut sich dabei durchschnittlich 19.6 Produkte an, legt davon 0.7 in den Einkaufswagen und kauft davon 0.3. 
Die meisten Sessions finden am Wochenende (Freitag-Sonntag) statt.


## Korrelationen
Im letzten Schritt der Analyse werden die einzelnen Spalten nun innerhalb von Korrelationsmatrizen ins Verhältnis zu den Entscheidungsvariablen gesetzt und betrachtet. 
Hiermit sollen die vorherigen Erkenntnisse überprüft werden. Hierfür müssen die Spalten "hour", "dayofweek", "dayofmonth" und "category_class" in Kategorien one-hot-encoded werden.
In der Tageszeit-Analyse ergibt sich eine positive Korrelation zwischen der Tageszeit "Morgens"(6-12Uhr) und dem Umsatz, der gekauften Menge und der in den Warenkorb hinzugefügten Menge. 
Die meisten Views hingegen ergeben sich eher in den Abendstunden zwischen 18 und 24 Uhr.
Die Wochentags-Analyse erkannt ebenfalls eine positive Korrelation zwischen dem Sonntag und dem Umsatz, sowie der gekauften Menge. Die meisten Views entstehen zwischen Montag und Donnerstag, während am zwischen Freitag und Sonntag am meisten Produkte in den Warenkorb gelegt werden.
In der Monats-Analyse wird eine positive Korrelation zwischen dem Ende des Monats(>=20.) und dem Umsatz und der gekauften Menge festgestellt. Angeschaut werden die Produkte überwiegend am Anfang und Ende des Monats und in den Warenkorb gelegt, in der Mitte des Monats. 
Die Erkenntnisse aus den Korrelationsanalysen decken sich grob mit den Ergebnissen aus der vorherigen Zeitpunkt-Analyse.
In der Kategorie-Analyse fällt wiederrum die positive Korrelation zwischen Electronics und Umsatz und gekaufter Menge auf. Besonders gerne werden zusätzlich zu den electronics noch die Klassen medicine und stationery in den Einkaufswagen gelegt. Die Klassen apparell, furniture und computers werden allerdings überwiegend nur angeschaut und eher selten gekauft.
Bei der Preis-Analyse ist erkennbar, dass ein hoher Preis auch zu einem hohen Umsatz, einer hohen gekauften Menge und einer hohen zu dem Warenkorb hinzugefügten Menge führt. Günstige Produkte werden hingegen eher nur angeschaut.

# Modellierung

## Kaufverhalten 
(Phillip)

## Kunden-Cluster mit K-Means 

### Datenvorbereitung
Um die Kundenklassifizierung umzusetzen werden die Insights aus der Data Exploration verwendet.
Als erstens werden das Feature-Engineering angewendet. Anschließend die Daten auf Session-Ebene aggregiert. Mithilfe dieser Daten wird daraufhin ein Kundenprofil für jeden 
Kunden angelegt (siehe ```src/data/data_preparation.py```).

Die Kundenprofile wurden dann in eine eigene Datei ausgelagert. Dadurch kann die Laufzeit deutlich verringert werden.
Für das Modelling werden nur Kunden verwendet, die insgesamt einen Umsatz von > 0 € getätigt haben.
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

<img src="https://raw.githubusercontent.com/BennerLukas/customeranalysis/main/src/data/exports/k-means_feature_dist_k4.png" alt="k_dist=10" width="800" align="center"/>

<img src="https://raw.githubusercontent.com/BennerLukas/customeranalysis/main/src/data/exports/k-means_feature_dist_k10.png" alt="k_dist=10" width="800" align="center"/>

(für eine interaktive Betrachtung der Plots öffne in ```src/data/exports``` die gewünschte .html Datei)

 ### Abschließende Interpretationen 
Die ermittelten Kundengruppen sind für k=4 und auch für k=10 mit eindeutigen Eigenschaften trennbar. 
Zum Beispiel werden Kunden die meist nur Produkte anschauen, aber so gut wie kaufen oder man findet Kunden, die sehr viel Umsatz im Verhältnis zu der Anzahl der Events getätigt haben (also spontane Käufer).
Es gibt aber auch das Gegenteil, also Kunden die erst sehr viel herumklicken und dann kaufen.
Mithilfe dieser identifizierten Kundengruppen soll (bzw. kann) nun Kundengruppen spezifisches Marketing betrieben werden.

Die einfache Erklärbarkeit, warum ein bestimmter Kunde einer bestimmten Gruppe zugeordnet wird, ist sehr gut. Dadurch kann das Modell jederzeit auf plausibilität überprüft werden.

Weitere Schritte um diese Data Exploration fortzuführen wären zum Beispiel:
- ein weiteres Modell testen (z.B. DBSCAN)
- mehr Features entwickeln und die Kombinationen miteinander testen (z.B. Anzahl Käufe je Produktkategorie)

# Weitere Schritte 
In den weiteren Schritten müssen die Erkenntnisse der vorherigen Analysen interpretiert werden, sodass auf Basis dieser Interpretationen Handlungsempfehlungen abgeleitet werden können. Beispielsweise ist erkennbar, dass die User tendenziell lieber teure Produkte kaufen, weshalb Rabatt-Aktionen vermutlich keine positive Auswirkungen zeigen werden. Außerdem fällt auf, dass die Klassen medicine und stationery oft in den Warenkorb gelegt werden, aber im Endeeffekt nicht gekauft werden. Hier könnte eine Erinnerungsmail helfen, um den User zum Abschluss des Kaufes zu bewegen.
Außerdem können für die einzelnen Kundengruppen spezifische Marketing-Aktionen ausgearbeitet werden. Dadurch fühlen sich die Kunden besser angesprochen, was sie zu einem höheren Umsatz bewegt.
