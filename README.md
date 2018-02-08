# Écriture d'un algorithme de partitionnement sous Spark
![alt text](https://spark.apache.org/images/spark-logo-trademark.png)

## Bibliothèques
__Healpix__:
HEALPix pour Hierarchical Equal Area isoLatitude Pixelisation est un algorithme de pixelisation.<br />	
La pixellisation peut être vu comme la mise en correspondance de la sphère avec douze losanges sur le plan,<br />	 suivie par la division binaire de ces losanges en pixels. Le logiciel associé HEALPix implémente l'algorithme. <br />
Sous JAVA Jhealpix.jar est la bibliothèque associée.

![alt text](http://healpix.sourceforge.net/html/introf1.png)

## Classe principal: PartitionRdd.java ![#f03c15](https://placehold.it/15/f03c15/000000?text=+)
### Contient les deux parties de l'algorithme
####  Partie I: Partitionnement Brute
####  Partie II Partitionnement Intelligent devant rassembler les cellules adjacents, les mémoriser dans un Buffer
####  puis effectuer le partitionnement selon ces cellules voisines.
### Map: Application du schéma au RDD

## AstroRecord.java: classe pour le RDD ![#1589F0](https://placehold.it/15/1589F0/000000?text=+)
##  HelloSpark.java: affiche ma version Spark  ![#1589F0](https://placehold.it/15/1589F0/000000?text=+)  
