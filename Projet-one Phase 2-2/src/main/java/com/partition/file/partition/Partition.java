package com.partition.file.partition;

//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.InputStream;
//import java.io.InputStreamReader;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
//import org.apache.spark.api.java.function.MapFunction;

//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Properties;

/**
 * Created by sene mouhamed fadel
 */

import healpix.essentials.*;
public class Partition {

	
	
	 public static void main(String[] args) {
		    SparkSession spark = SparkSession
		      .builder()
		      .appName("Algo de Partitionnement")
		      .config("spark.master", "local")
		      .getOrCreate();

		    
		    PartitionCsv(spark);
		  // spark.stop();
		    
		    
		  }
	 
	
	 
	 
	 private static void PartitionCsv(SparkSession spark) {
		 
		 //Schema
		 Dataset<Row> astroDataframe = spark.read().json("/home/vaadl/Bureau/test/schema.json");
		 
		//Chargement du fichier
		 Dataset<Row> astroDataframeCsv = spark.read().format("csv")
				 .option("sep", ";") 
				 .option("inferSchema", "true")
			     .option("header", "true")
			     .load("/home/vaadl/Bureau/test/N050k.csv");
		 
		 //Visualisation du schema
		 astroDataframe.printSchema();
		 astroDataframe.show();
		 astroDataframe.select("name").show();
		 
		 
		 //Visualisation du Dfcsv
		 astroDataframeCsv.show();
		 
		 //Selection des colonne alpha & delta
		// astroDataframeCsv.select("alpha", "delta").write().format("parquet").save("coordonnees.parquet");
		 
		 //Sauvegarde du Dataframe dans un fichier csv
		 astroDataframe.write().parquet("/home/vaadl/Bureau/test/astro.parquet");
		 
		 
		//************HEALPIX***************/
			//Partition du fichier
			System.out.printf("Usage healpix");
		    int nSide = 16;
		    
		    try {
		    	HealpixBase healpixBase = new HealpixBase(nSide, Scheme.NESTED);
		    	double alpha=179.16863296831528;
		    	double delta = -54.818955882435255;
		    	double theta = Math.PI / 2 - Math.toRadians(delta);
		    	double phi = Math.toRadians(alpha);
		        
		    	 long res = healpixBase.ang2pix(new Pointing(theta, phi));
                 System.out.printf("Pix %d= %d", nSide, res);
		    	
            // List<Double> list = astroDf.as(Encoders.double()).collectAsList();
             //Dataset<double> astroDf1 = session.createDataset(list, Encoders.double());
		   // astroDf = healpixBase.ang2pix(new Pointing(theta, phi));
		      //System.out.printf("Pix %d= %d", nSide, astroDataframe);
		    }
		    catch(Exception e){
		    	e.printStackTrace();
		    }
		 
		/* astroDataframe
		 .write()
		  .partitionBy("name")
		  .saveAsTable("partitions_schema_N050k");*/
		 
		 // Push du Dataframe dans une vue
		 astroDataframeCsv.createOrReplaceTempView("astroDfparquet");
		 Dataset<Row> coordonneesDf = spark.sql("SELECT name FROM astroDfparquet");
		
		 
		 coordonneesDf.show();
		 /*Dataset<String> coordo = coordonneesDf.map(
			        (MapFunction<Row, String>) row -> "name: " + row.getString(0),
			        Encoders.STRING());
			    coordo.show();*/
		/* astroDataframeCsv
		 		  .write()
		 		  .partitionBy("res")
		 		  .saveAsTable("partitions_N050k");*/
		 		  
		 
	 }
	 
	 
}
