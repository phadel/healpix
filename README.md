# Écriture d'un algorithme de partitionnement sous Spark
![alt text](https://spark.apache.org/images/spark-logo-trademark.png)

## Bibliothèques
__Healpix__:
HEALPix pour Hierarchical Equal Area isoLatitude Pixelisation est un algorithme de pixelisation.<br />	
La pixellisation peut être vu comme la mise en correspondance de la sphère avec douze losanges sur le plan,<br />	 suivie par la division binaire de ces losanges en pixels. Le logiciel associé HEALPix implémente l'algorithme. <br />
Sous JAVA Jhealpix.jar est la bibliothèque associée.

![alt text](http://healpix.sourceforge.net/html/introf1.png)

### PartitionRdd.java 
PartitionRdd.java 	<br />
Utilisation d'un RDD puis d'une DataFrame pour le chargement et la structuration des données.	<br />	
Avec une requête sur le DataFrame j'obtient par exemple: Dataset<Row> sqlDF = spark.sql("SELECT alpha, delta FROM astro"); <br />		
  astro est une vue dans laquelle j'ai pushé le DataFrame	<br />	
+------------------+-------------------+		<br />
|    alpha		|              delta|		<br />
+------------------+-------------------+		<br />
| 179.1741777461607| -54.84440852225688|		<br />
|179.16863296831528|-54.818955882435255|		<br />
|179.14345263705383|-54.852296681036876|		<br />
| 179.1838233457786| -54.83488031056781|		<br />
|179.18096292186576| -54.83488677329097|		<br />
|179.19062651245682| -54.83486493976191|		<br />
| 179.1777554629883| -54.84748160011696|		<br />
| 179.1777534301612| -54.84748072945883|		<br />
|179.17775343236244|-54.847480742430605|		<br />
|179.17775762229473|-54.847482521945985|		<br />
|179.15971648985104| -54.83337827260119|		<br />
|179.20102454536746| -54.85485916460365|		<br />
|179.20102471367406| -54.85485925324877|		<br />
| 179.2010243337422| -54.85485905316895|		<br />
|179.16399884422177|-54.830533176350166|		<br />
| 179.1639972367133|-54.830533047771944|		<br />
|179.16400153313995| -54.83053339143339|		<br />
|179.16177661236105| -54.82782818682312|		<br />
|179.15931133141905| -54.83327730024494|		<br />
|179.16092548299753|-54.844137725676354|		<br />
+------------------+-------------------+		<br />
only showing top 20 rows		<br />
<br />
