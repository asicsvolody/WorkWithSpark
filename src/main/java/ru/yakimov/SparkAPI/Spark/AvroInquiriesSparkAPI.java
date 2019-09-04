package ru.yakimov.SparkAPI.Spark;

import com.google.gson.Gson;
import javassist.NotFoundException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import ru.yakimov.SparkAPI.WorkWhithFiles.UserWithData;

import java.io.Serializable;
import java.util.List;


public class AvroInquiriesSparkAPI implements Serializable {

    private JavaRDD<UserWithData> data;

    private String avroPath = "/java/projects/WorkWithSpark/src/main/resources/User.avro";
    private String schemaPath = "/java/projects/WorkWithSpark/src/main/resources/UserWithData.avsc";
    JavaSparkContext sc = AvroInquiriesSparkSQL.sc;
    AvroUsersSaver aSaver;



    public AvroInquiriesSparkAPI() {

        sc.setLogLevel("WARN");

        aSaver = new AvroUsersSaver(avroPath, schemaPath);


        data = sc.newAPIHadoopFile(
                avroPath
                , AvroKeyInputFormat.class,AvroKey.class,UserWithData.class
                ,sc.hadoopConfiguration())
                .keys()
                .map(v-> avroDecoding(v.toString()))
                .persist(StorageLevel.MEMORY_AND_DISK());
    }


    public String getPhoneFromName(String name) throws NotFoundException {
        JavaRDD<String> res = data.filter(v-> v.getName().equals(name))
                .map(UserWithData::getPhone);
        if(res.isEmpty()) {
            throw new NotFoundException("file not found");
        }
        return res.first();



    }

    public boolean setPhoneForName(String name, String phone){
        JavaRDD<UserWithData> userForChange = data
                .filter(v -> v.getName().equals(name))
                .map(v -> {
                    v.setPhone(phone);
                    return v;
                });

        if(userForChange.isEmpty())
            return false;

        data = data
                .filter(v -> !v.getName().equals(name))
                .union(userForChange)
                .persist(StorageLevel.MEMORY_AND_DISK());
        recordToAvro();
        return true;
    }





    public static UserWithData avroDecoding(String json){
        return  new Gson()
                .fromJson(json,UserWithData.class);
    }

    private void recordToAvro(){
        aSaver.saveToAvro(data.collect());
    }

//    public boolean changeDataForId(Long id, List<Object> list){
//
//    }

}
