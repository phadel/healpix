package com.partition.file.partition;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import healpix.essentials.*;
import healpix.essentials.Scheme;




public class PartitionRdd {
	

	//private static final String LongType = null;

	public static void main(String[] args) {
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("Algo de Partitionnement RDD")
	      .config("spark.master", "local")
	      .getOrCreate();

	    
	    AstroRecordCsv(spark);
	    spark.stop();
	    
	    
	  }
	
	 private static void AstroRecordCsv(SparkSession spark) {
		 
	// Création d'un RDD AstoRecord depuis notre csv
	JavaRDD<AstroRecord> astroRecordRDD = spark.read()
	  .textFile("/home/vaadl/Bureau/test/N050k.csv")
	  .javaRDD()
	  .map(line -> {
	    String[] parts = line.split(",");
	    AstroRecord astrorecord = new AstroRecord();
	    astrorecord.setNc(Integer.parseInt (parts[0].trim()));
	    astrorecord.setNt(Integer.parseInt(parts[1].trim()));
	    astrorecord.setHost(Integer.parseInt(parts[2].trim()));
	    astrorecord.setAlpha(Double.parseDouble(parts[3].trim()));
	    astrorecord.setDelta(Double.parseDouble(parts[4].trim()));
	    astrorecord.setDistance(Double.parseDouble(parts[5].trim()));
	    astrorecord.setMuAlpha(Double.parseDouble(parts[6].trim()));
	    astrorecord.setMuDelta8(Double.parseDouble(parts[7].trim()));
	    astrorecord.setRadialVelocity(Double.parseDouble(parts[8].trim()));
	    astrorecord.setMagG(Double.parseDouble(parts[9].trim()));
	    astrorecord.setMagGBp(Double.parseDouble(parts[10].trim()));
	    astrorecord.setMagGRp(Double.parseDouble(parts[11].trim()));
	    astrorecord.setMagGRvs(Double.parseDouble(parts[12].trim()));
	    astrorecord.setAv(Double.parseDouble(parts[13].trim()));
	    astrorecord.setAg(Double.parseDouble(parts[14].trim()));
	    astrorecord.setRv(Double.parseDouble(parts[15].trim()));
	    astrorecord.setMeanAbsoluteV9(Double.parseDouble(parts[16].trim()));
	    astrorecord.setColorVminusI(Double.parseDouble(parts[17].trim()));
	    astrorecord.setOrbitPeriod(Double.parseDouble(parts[18].trim()));
	    astrorecord.setPeriastronDate(Double.parseDouble(parts[19].trim()));
	    astrorecord.setSemimajorAxis(Double.parseDouble(parts[20].trim()));
	    astrorecord.setEccentricity(Double.parseDouble(parts[21].trim()));
	    astrorecord.setPeriastronArgument(Double.parseDouble(parts[22].trim()));
	    astrorecord.setInclination(Double.parseDouble(parts[23].trim()));
	    astrorecord.setLongitudeAscendingNode(Double.parseDouble(parts[24].trim()));
	    astrorecord.setPhase(Double.parseDouble(parts[25].trim()));
	    astrorecord.setFlagintegereracting(Integer.parseInt (parts[26].trim()));
	    astrorecord.setPopulation(Integer.parseInt (parts[27].trim()));
	    astrorecord.setAge(Double.parseDouble(parts[28].trim()));
	    astrorecord.setFeH(Double.parseDouble(parts[29].trim()));
	    astrorecord.setAlphaFe(Double.parseDouble(parts[30].trim()));
	    astrorecord.setMbol(Double.parseDouble(parts[31].trim()));
	    astrorecord.setMass(Double.parseDouble(parts[32].trim()));
	    astrorecord.setRadius(Double.parseDouble(parts[33].trim()));
	    astrorecord.setTeff(Double.parseDouble(parts[34].trim()));
	    astrorecord.setLogg(Double.parseDouble(parts[35].trim()));
	    astrorecord.setSpectralType(parts[36]);
	    astrorecord.setVsini(Double.parseDouble(parts[37].trim()));
	    astrorecord.setrEnvRStar(Double.parseDouble(parts[38].trim()));
	    astrorecord.setBondAlbedo(Double.parseDouble(parts[39].trim()));
	    astrorecord.setGeomAlbedo(Double.parseDouble(parts[40].trim()));
	    astrorecord.setVariabilityType(parts[41]);
	    astrorecord.setVariabilityAmplitude(Double.parseDouble(parts[42].trim()));
	    astrorecord.setVariabilityPeriod(Double.parseDouble(parts[43].trim()));
	    astrorecord.setVariabilityPhase(Double.parseDouble(parts[44].trim()));
	    astrorecord.setHasPhotocenterMotion(Boolean.parseBoolean(parts[45].trim()));
	    astrorecord.setOpenClusterName(parts[46]);
	    astrorecord.setSourceExtendedId(parts[47]);
	    astrorecord.setSourceId(parts[48]);
	    astrorecord.setHealpix(parts[49]);
	    return astrorecord;
	  });
	
	
	//Application du schéma au RDD de JavaBeans pour obtenir un DataFrame
	Dataset<Row> astroDF = spark.createDataFrame(astroRecordRDD, AstroRecord.class);
	
	astroDF
	.write()
	.format("parquet")
 	.save("/home/vaadl/Bureau/test/astroDF.parquet");
	//astroDF.show();
	Dataset<Row>  astroDFparquet=spark.sql("SELECT * FROM parquet.`/home/vaadl/Bureau/test/astroDF.parquet`");
	//astroDFparquet.show();
	
	
	//astroDF.write().parquet("/home/vaadl/Bureau/test/astroRDD.parquet");
	
	// Push du DF dans une vue
	astroDFparquet.createOrReplaceTempView("astro");
	
	// Essais SQL
	Dataset<Row> sqlDF = spark.sql("SELECT alpha, delta FROM astro");
	System.out.printf("\n");
	System.out.printf("********Affichage des coordonnées de l'étoile alpha & delta********");
	System.out.printf("\n");
	sqlDF.show();
	
	astroDFparquet.select( astroDFparquet.col("alpha").cast(DataTypes.LongType).as("alphaPix"), astroDFparquet.col("delta").cast(DataTypes.LongType).as("deltaPix")).show();
	
	
	
	// Utilisation des UDFs
	//spark.udf().register("COORDO", new UDF1<Long, Long>() {
		  /**
		 * 
		 */
		/*private static final long serialVersionUID = 1L;

		@Override
		  public Long call(Long alpha) {
		    return (((long) alpha));
		  }
		}, DataTypes.LongType);
	spark.sql("SELECT alpha, COORDO(alpha) AS alphaPix FROM astro").show();*/
    	
    	//Dataset<Row> coordoDF = astroDFparquet.withColumn("alphaTmp", astroDF.col("alpha").cast(LongType)).drop("alpha").withColumnRenamed("alphaTmp", "alpha");   
    	//coordoDF = astroDFparquet.withColumn("deltaTmp", astroDF.col("delta").cast(LongType)).drop("delta").withColumnRenamed("deltaTmp", "delta");   
    	
    	//coordoDF.show();
    	
    	//coordoDF
    	//.write()
    	//.format("parquet")
     	//.save("/home/vaadl/Bureau/test/coordoDF.parquet");
    	
    	
    	/*astroDFparquet.col("alpha.field");
    	astroDFparquet.col("delta.field");
    	astroDFparquet.show();*/
    	
    	//long alphaPx;
    	
    	//alphaPix=alphaPx;
    	//alphaPx=alphaPix;
		//Column deltaPix = coordoDF.col("deltaTmp");
    	
	//	long  alphaPixx = alphaPix.Field<long>("alphaTmp"); 
    	
	//Partitionnement healpix
	System.out.printf("Usage healpix");
    int nSide = 16;
    
    try {
    	HealpixBase healpixBase = new HealpixBase(nSide, Scheme.NESTED);
    	
    	double theta = Math.PI / 2 - Math.toRadians(-54);
        double phi = Math.toRadians(179);
        
    	long res = healpixBase.ang2pix(new Pointing(theta,phi));
    	System.out.printf("\n");
    	System.out.printf("Pix");
    	System.out.printf("\n");
    	System.out.printf("%d", nSide);
    	System.out.printf("\n");
    	System.out.printf("%d", res);
        //System.out.printf("Pix %d= %d", nSide, res);
    	
    	 Dataset<Row> healPixDF = astroDFparquet.withColumn("reshealpix", functions.lit(res));   
    	 
    	 healPixDF.select("reshealpix").show();
    	 
    	 healPixDF
    		.write()
    		.partitionBy("reshealpix")
    		.saveAsTable("partitions_reshealpix_N050k");
    	
    }
    catch(Exception e){
    	e.printStackTrace();
    }
	
	
    
	 }
	 
 	
}
