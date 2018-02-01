import org.apache.spark.HashPartitioner

val textFile = sc.textFile("file:/home/vaadl/healpix-partition/SparkQL/N050k.csv")
val count = textFile.flatMap(line=>line.split("")).map(word=>(word,1)).partitionBy(new HashPartitioner(10))

count.reduceByKey(_+_).saveAsTextFile("/home/vaadl/healpix-partition/SparkQL/PartitionAstro1")
