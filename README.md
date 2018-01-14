# healpix
Project One: Contient partie java(utilisation de la bibliothèque healpix)
Fichier principal: /Projet-one/src/main/java/com/partition/file/partition/Partition.java
lie pour le moment le fihier pris en entrée (N050k.csv)
SparkQL: Contient le partitionnement SparkQL
J'utiilise ici la fonction HashPartitionner. Le paramètre transmis à HashPartitioner définit le nombre de partitions (10).
L'ordre des valeurs après le shuffle est non déterministe.
