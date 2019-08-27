package ru.yakimov.SparkAPI.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;



public class AvroInquiries {
    private SQLContext sqlContext;
    private Dataset<Row> ds;

    public AvroInquiries() {
        sqlContext = new SQLContext(new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("MyApp")));
        ds = sqlContext
                .read()
                .format("avro")
                .load("/java/projects/WorkWithSpark/src/main/resources/User.avro")
                .sort("id")
                .persist(StorageLevel.MEMORY_AND_DISK());
    }

    public Dataset<Row> getNameForId(Long id){
        return  ds.select("name")
                .where("id="+id);
    }

    public Dataset<Row> getPhoneForName(String name){
        ds.createOrReplaceTempView("people");
        return sqlContext.sql(String.format("SELECT phone FROM people WHERE name = '%s'", name));
    }


    public Dataset<Row> getDs() {
        return ds;
    }
}

