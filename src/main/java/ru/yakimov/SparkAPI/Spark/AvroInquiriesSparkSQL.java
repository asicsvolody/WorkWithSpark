package ru.yakimov.SparkAPI.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;
import ru.yakimov.SparkAPI.WorkWhithFiles.UserWithData;

import java.util.Arrays;


public class AvroInquiriesSparkSQL {
    private SQLContext sqlContext;
    private Dataset<Row> ds;

    public AvroInquiriesSparkSQL() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("MyApp"));
        sc.setLogLevel("WARN");
        sqlContext = new SQLContext(sc);

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
    public Dataset<Row> getIdForName(String name){
        return  ds.select("id")
                .where(String.format("name='%s'",name));
    }

    public Dataset<Row> getPhoneForName(String name){
        ds.createOrReplaceTempView("people");
        return sqlContext.sql(String.format("SELECT phone FROM people WHERE name = '%s'", name));
    }

    public Dataset<Row> getUserWithName(String name){
        ds.createOrReplaceTempView("people");
        return sqlContext.sql(String.format("SELECT * FROM people WHERE name = '%s'", name));
    }

//    public void setPhoneForName(String name, String phone){
//        ds.createOrReplaceTempView("people");
//
//        Long userId = getIdForName(name).as(Encoders.LONG()).first();
//
//
//        Dataset<Row>  userToChenge = sqlContext.sql("CREATE TABLE ``.`new_table` ( `id`,`name`,`phone` TEXT(200)");
//    }


    public Dataset<Row> getDs() {
        return ds;
    }
}

