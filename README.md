# healpix


PartitionRdd.java 	
Utilisation d'un RDD puis d'une DataFrame pour le chargement et la structuration des données.		
Avec une requête sur le DataFrame j'obtient par exemple: Dataset<Row> sqlDF = spark.sql("SELECT alpha, delta FROM astro");		
  astro est une vue dans laquelle j'ai pusher le DataFrame		
+------------------+-------------------+		
|             alpha|              delta|		
+------------------+-------------------+		
| 179.1741777461607| -54.84440852225688|		
|179.16863296831528|-54.818955882435255|		
|179.14345263705383|-54.852296681036876|		
| 179.1838233457786| -54.83488031056781|		
|179.18096292186576| -54.83488677329097|		
|179.19062651245682| -54.83486493976191|		
| 179.1777554629883| -54.84748160011696|		
| 179.1777534301612| -54.84748072945883|		
|179.17775343236244|-54.847480742430605|		
|179.17775762229473|-54.847482521945985|		
|179.15971648985104| -54.83337827260119|		
|179.20102454536746| -54.85485916460365|		
|179.20102471367406| -54.85485925324877|		
| 179.2010243337422| -54.85485905316895|		
|179.16399884422177|-54.830533176350166|		
| 179.1639972367133|-54.830533047771944|		
|179.16400153313995| -54.83053339143339|		
|179.16177661236105| -54.82782818682312|		
|179.15931133141905| -54.83327730024494|		
|179.16092548299753|-54.844137725676354|		
+------------------+-------------------+		
only showing top 20 rows		

Utilisation des UDF pour changer le type de alpha et delta		
// Utilisation des UDFs		
	spark.udf().register("COORDO", new UDF1<Long, Long>() {		
		  /**		
		 * 		
		 */		
		private static final long serialVersionUID = 1L;		

		@Override		
		  public Long call(Long alpha) {		
		    return (((long) alpha));		
		  }		
		}, DataTypes.LongType);		
	spark.sql("SELECT alpha, COORDO(alpha) AS alphaPix FROM astro").show();		
Renvoie un message:		
org.apache.spark.SparkException: Failed to execute user defined function($anonfun$27: (double) => bigint)			
SparkQL__		
#######################__		
Contient le partitionnement SparkQL__		
J'utiilise ici la fonction HashPartitionner. Le paramètre transmis à HashPartitioner définit le nombre de partitions (10).

