import org.apache.spark.{ SparkConf, SparkContext } 
import org.apache.spark.sql.SQLContext


val conf = new SparkConf().setAppName(AppName).setMaster(master) 
val sc = SparkContext.getOrCreate(conf) 

val sqlContext = SQLContext.getOrCreate(sc) 
val df = spark.read.format("csv").option("header", "true").load("/home/vaadl/Bureau/hpy/N050k.csv")
val df2 = sqlContext.read.json ("/home/vaadl/Bureau/hpy/schema.json")

df.write.
format("csv").
mode("append").
partitionBy("???").csv("/home/vaadl/Bureau/hpy/parts")





//partitionBy("age")

/*Création du DataFrame
Chargement du fichier csv comme dataframe
https://stackoverflow.com/questions/29704333/spark-load-csv-file-as-dataframe

val df = sqlContext.read.csvFile("/home/vaadl/Bureau/hpy/N050k.csv")

Ajustement affichage DataFrame: spark.debug.maxToStringFields=10
*/
