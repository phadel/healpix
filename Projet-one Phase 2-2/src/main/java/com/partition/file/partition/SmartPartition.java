package com.partition.file.partition;

import java.util.ArrayList;
import java.util.List;

import java.util.Arrays;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaSparkContext;

import healpix.essentials.*;
import scala.reflect.ClassTag;

/**
 * Created by sene mouhamed fadel
 */


public class SmartPartition {
	
	public static void main(String[] args) {
		    
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("Smart Partitionning")
	      .config("spark.master", "local")
	      .getOrCreate();

	    spark.sparkContext().conf().set("spark.driver.allowMultipleContexts","true");
	    AstroRecordCsv2(spark);
	    spark.stop();
	    
	    
	  }
	
	
	private static void AstroRecordCsv2(SparkSession spark) {
		
		
		/************DataFrame************/
		// Création RDD
		JavaRDD<String> astroRDD = spark.sparkContext()
		  .textFile("/home/vaadl/Bureau/test/N050k.csv", 1)
		  .toJavaRDD();
		
		
		//Schéma
		// Dataset<Row> schema = spark.read().json("/home/vaadl/Bureau/test/schema.json");
		String schemaString =    "nc nt host alpha delta distance muAlpha muDelta8 radialVelocity magG magGBp magGRp magGRvs Av Ag Rv meanAbsoluteV9 colorVminusI orbitPeriod periastronDate "
				+ "semimajorAxis eccentricity periastronArgument inclination longitudeAscendingNode phase flagegereracting"
				+"population age feH alphaFe mbol mass radius teff logg spectralType vsini rEnvRStar bondAlbedo geomAlbedo variabilityType"
				+"variabilityAmplitude variabilityPeriod variabilityPhase hasPhotocenterMotion openClusterName sourceExtendedId sourceId healpix";
		
		// Génération du schéma en fonction de la chaîne "schemaString"
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
		  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
		  fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		
		// Conversion des enregistrements du RDD (astroRDD) en Lignes
		JavaRDD<Row> rowRDD = astroRDD.map((Function<String, Row>) record -> {
		  String[] attributes = record.split(",");
		  return RowFactory.create(attributes[0].trim(), attributes[1].trim(), attributes[2].trim(), attributes[3].trim(), attributes[4].trim(),attributes[5].trim(),attributes[6].trim(),attributes[7].trim(),attributes[8].trim(),attributes[9].trim(),attributes[10].trim(),
				  attributes[11].trim(),attributes[12].trim(),attributes[13].trim(),attributes[14].trim(),attributes[15].trim(),attributes[16].trim(),attributes[17].trim(),attributes[18].trim(),attributes[19].trim(),attributes[20].trim(),
				  attributes[21].trim(),attributes[22].trim(),attributes[23].trim(),attributes[24].trim(),attributes[25].trim(),attributes[26].trim(),attributes[27].trim(),attributes[28].trim(),attributes[29].trim(),attributes[30].trim(),
				  attributes[31].trim(),attributes[32].trim(),attributes[33].trim(),attributes[34].trim(),attributes[35].trim(),attributes[36].trim(),attributes[37].trim(),attributes[38].trim(),attributes[39].trim(),attributes[40].trim(),
				  attributes[41].trim(),attributes[42].trim(),attributes[43].trim(),attributes[44].trim(),attributes[45].trim(),attributes[46].trim(),attributes[47].trim(),attributes[48].trim(),attributes[49].trim());
		});
		// Application du schéma au RDD
		 Dataset<Row> astroDataFrame = spark.createDataFrame(rowRDD, schema);
		 
		 astroDataFrame.show();
		 
		 
		/************DataFrame************/
		
		 
		 
		 /************Healpix************/
		System.out.printf("Test healpix 1");
	    int nSide = 16;
	    try {
	        HealpixBase hp = new HealpixBase(nSide, Scheme.NESTED);
	
	        double alpha=179;
	        double delta = -54;
	        double theta = Math.PI / 2 - Math.toRadians(delta);
	        double phi = Math.toRadians(alpha); 
	        Pointing vec=new Pointing(theta, phi);
	        long ipix = hp.ang2pix(vec); //Retourne le nb de pixel autour du pt vec
	
	        //System.out.printf("Pix %d", ipix);
	        long [] nei= hp.neighbours(ipix);
	        
	        //Conversion du tableau en liste
	        List<long[]> data = Arrays.asList(nei);
	        
	        //conversion array
	        String[] nei_string = new String[nei.length];
	        
	        for(int i = 0; i <nei.length; i++){
	        	nei_string[i] = String.valueOf(nei[i]);
	        }
	        
	        //Conversion de la liste en RDD parallelize
	        /*SparkConf conf = new SparkConf().setAppName("Smart Partitionning1").setMaster("local");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        sc.getConf().set("spark.driver.allowMultipleContexts","true");*/
	        //JavaRDD<long[]> neiRDD = sc.parallelize(data);
	        //Dataset<Row> dataDF= spark.createDataset(nei_string,  Encoders.STRING()).toDF();
	      //  JavaRDD<long[]> neiRDD =  spark.sparkContext().parallelize(data, 50, scala.reflect.ClassManifestFactory.fromClass(List[].class));
	        
	        //
	      // JavaRDD<Row> rowRDD2 = neiRDD.map((Function<long[], Row>) record -> {
	  		 //int attributes = record.length;
	  		//Long[] attributes = record.wait();
	  		
	  		/* return RowFactory.create(nei[0], nei[1], nei[2], nei[3], nei[4],nei[5],nei[6],nei[7],nei[8],nei[9],nei[10],
	  				nei[11],nei[12],nei[13],nei[14],nei[15],nei[16],nei[17],nei[18],nei[19],nei[20],
	  				nei[21],nei[22],nei[23],nei[24],nei[25],nei[26],nei[27],nei[28],nei[29],nei[30],
	  				nei[31],nei[32],nei[33],nei[34],nei[35],nei[36],nei[37],nei[38],nei[39],nei[40],
	  				nei[41],nei[42],nei[43],nei[44],nei[45],nei[46],nei[47],nei[48],nei[49]);
	  		});*/
	        
	        // DataFrame
	        //Dataset<Row> neiDF = spark.createDataFrame(rowRDD2, schema);
	        //neiDF.show();
	        
	      
	        //System.out.printf("neighbours %s", Arrays.toString(nei));
	       
	//        System.out.printf("neighbours %s", (nei[0]));
	
	        
	       /* RangeSet rs=hp.queryDisc(vec,0.1); //renvoie la liste des
	       // numéros de cellules couvrant le cône (long, lat, radius)
	        System.out.printf("queryDisc = %s",rs.toString());
	
	        System.out.printf("queryDisc = %s",Arrays.toString(rs.toArray()));*/
	        
	      /*  Dataset<Row> healPixDF2 = astroDataFrame.withColumn("neighbours", functions.lit(nei));   
	        
	        healPixDF2.select("neighbours").show();
	      healPixDF2
    		.write()
    		.partitionBy("neighbours");*/
    		//.saveAsTable("partitions_neighbours_N050k");
	        
	        //Création d'un nouveau DataFrame à partir de notre variable array (nei)
	        //List<StructField> fields = new ArrayList<>();
	        //StructType schema = DataTypes.createStructType(fields);
	       
	       /*JavaRDD<Row> rowRDD2 = nei.map((Function<String, Row>) record -> {
	 		  String[] attributes = record.split(",");
	 		  return RowFactory.create(attributes[0].trim(), attributes[1].trim(), attributes[2].trim(), attributes[3].trim(), attributes[4].trim(),attributes[5].trim(),attributes[6].trim(),attributes[7].trim(),attributes[8].trim(),attributes[9].trim(),attributes[10].trim(),
	 				  attributes[11].trim(),attributes[12].trim(),attributes[13].trim(),attributes[14].trim(),attributes[15].trim(),attributes[16].trim(),attributes[17].trim(),attributes[18].trim(),attributes[19].trim(),attributes[20].trim(),
	 				  attributes[21].trim(),attributes[22].trim(),attributes[23].trim(),attributes[24].trim(),attributes[25].trim(),attributes[26].trim(),attributes[27].trim(),attributes[28].trim(),attributes[29].trim(),attributes[30].trim(),
	 				  attributes[31].trim(),attributes[32].trim(),attributes[33].trim(),attributes[34].trim(),attributes[35].trim(),attributes[36].trim(),attributes[37].trim(),attributes[38].trim(),attributes[39].trim(),attributes[40].trim(),
	 				  attributes[41].trim(),attributes[42].trim(),attributes[43].trim(),attributes[44].trim(),attributes[45].trim(),attributes[46].trim(),attributes[47].trim(),attributes[48].trim(),attributes[49].trim());
	 		});*/
	        //Dataset<Row> neiDF = spark.createDataFrame(nei,schema);
	
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
		 
		 /************Healpix************/
	}
}
