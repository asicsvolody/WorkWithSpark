//package ru.yakimov.SparkAPI;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.*;
//
//import org.apache.spark.sql.hive.HiveContext;
//
//
//public class AvroInquiries {
//    private SQLContext sqlContext;
//
//    public AvroInquiries() {
//        sqlContext = new HiveContext(new JavaSparkContext(
//                new SparkConf().setMaster("local").setAppName("MyApp")));
//    }
//
//    public String getNameForId(Long id){
//        Datas
//        DataFrame df = sqlContext.read().format("com.databricks.spark.avro")
//                .load("src/test/resources/episodes.avro");
//
//    }
////
////
////
//}

