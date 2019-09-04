package ru.yakimov.SparkAPI.Spark;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.avro.AvroOutputWriter;
import org.apache.spark.storage.StorageLevel;
import ru.yakimov.SparkAPI.WorkWhithFiles.UserWithData;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;


public class AvroInquiriesSparkSQL implements Serializable{



    private String avroPath = "/java/projects/WorkWithSpark/src/main/resources/User.avro";
    private String avroPathToSave = "/java/projects/WorkWithSpark/src/main/resources/User";

    private String schemaPath = "/java/projects/WorkWithSpark/src/main/resources/UserWithData.avsc";

    private SQLContext sqlContext;
    private Dataset<Row> ds;
    AvroUsersSaver aSaver;
    public static JavaSparkContext sc = new JavaSparkContext(
            new SparkConf().setMaster("local").setAppName("MyApp"));

    public AvroInquiriesSparkSQL() {

        sc.setLogLevel("WARN");
        sqlContext = new SQLContext(sc);

        aSaver = new AvroUsersSaver(avroPath, schemaPath);

        ds = sqlContext
                .read()
                .format("avro")
                .load(avroPath)
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


    public Dataset<Row> getDs() {
        return ds;
    }


    public boolean setPhoneForName(String name, String newPhone){
        ds.createOrReplaceTempView("people");
        Dataset<Row> user = sqlContext.sql(String.format("SELECT * FROM people WHERE name = '%s'", name));
        Long id =  user
                .select("id")
                .as(Encoders.LONG())
                .first();
        if(id == null)
            return false;

        JavaRDD<UserWithData> newUser = sc.parallelize(Collections.singletonList(new UserWithData(id, name, newPhone)));
        JavaRDD<UserWithData> usersRdd =  sqlContext
                .sql(String.format("SELECT * FROM people WHERE name != '%s'", name))
                .toJavaRDD()
                .map(v -> new UserWithData(v.getLong(0),v.getString(1),v.getString(2)))
                .union(newUser);
        aSaver.saveToAvro(usersRdd.collect());
        ds = sqlContext.createDataFrame(usersRdd, UserWithData.class).sort("id").persist(StorageLevel.MEMORY_AND_DISK());


        File saveDirecroty = new File(avroPathToSave);
        if(saveDirecroty.exists()) {
            try {
                FileUtils.deleteDirectory(saveDirecroty);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        ds.coalesce(1).write().format("avro").save(saveDirecroty.getPath());
        return true;
    }


}

