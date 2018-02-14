package com.partition.file.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by sene mouhamed fadel
 */

/*Class HelloSpark affichant ma version spark*/
public class HelloSpark {
	 public static void main(String[] args) {
	        SparkConf conf = new SparkConf().setAppName("Hello Spark").setMaster("local");
	        SparkContext sc = new SparkContext(conf); 
	        System.out.println("Hello, Spark v." + sc.version());
	    }
	 
}
