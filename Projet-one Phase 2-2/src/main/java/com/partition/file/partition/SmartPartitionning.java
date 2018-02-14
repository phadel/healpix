package com.partition.file.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



import healpix.essentials.HealpixBase;
import healpix.essentials.Pointing;
import healpix.essentials.Scheme;

public class SmartPartitionning {

	public static void main(String[] args) {
	    SparkConf conf = new SparkConf().setAppName("SparkSample").setMaster("local[*]");
	    JavaSparkContext jsc = new JavaSparkContext(conf);
	    @SuppressWarnings("deprecation")
		SQLContext sqc = new SQLContext(jsc);
	    // DataFrame
	    
	 // Création RDD
	 		JavaRDD<String> astroRDD = sqc.sparkContext()
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
			 Dataset<Row> astroDataFrame = sqc.createDataFrame(rowRDD, schema);
			 
			 astroDataFrame.show();
	    
			//astroDataFrame.sample(true, 2D*50000/astroDataFrame.count());
			 
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
			        List<String> nei_string = new ArrayList<String>(data.size());
			        
			        for(int i = 0; i <data.size(); i++){
			        	nei_string.add(i, String.valueOf(data.get(i))); 
			        	
			        	//nei_string = String.valueOf(data.get(i));
			        	//Log.i(i,data.get(i));
			        }
			        
			        //Conversion de la liste en RDD parallelize
			        /*SparkConf conf = new SparkConf().setAppName("Smart Partitionning1").setMaster("local");
			        JavaSparkContext sc = new JavaSparkContext(conf);
			        sc.getConf().set("spark.driver.allowMultipleContexts","true");*/
			        JavaRDD<long[]> neiRDD = jsc.parallelize(data);
			        Dataset<Row> dataDF= sqc.createDataset(nei_string,  Encoders.STRING()).toDF("Neighbours");
			        dataDF.show();
			       
			        //
			       JavaRDD<Row> rowRDD2 = neiRDD.map((Function<long[], Row>) record -> {
			  		 //int attributes = record.length;
			  		//Long[] attributes = record.split("|");
			  		long attributes = record.length;
			  		
			  		return RowFactory.create(attributes);
			  		});
			        
			        // DataFrame
			       Dataset<Row> neiDF = sqc.createDataFrame(rowRDD2, schema);
			       //Dataset<Row> neiDF = sqc.crea
			        neiDF.show();
			        
			      
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
	    // Convert
	    
	   }
}
