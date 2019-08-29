package ru.yakimov.SparkAPI.Spark;

import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import ru.yakimov.SparkAPI.WorkWhithFiles.UserWithData;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;


public class AvroInquiriesSparkAPI implements Serializable {

    private JavaRDD<UserWithData> data;
    private Schema avroSchema ;
    private String avroPath = "/java/projects/WorkWithSpark/src/main/resources/User.avro";
    private String schemaPath = "/java/projects/WorkWithSpark/src/main/resources/UserWithData.avsc";



    public AvroInquiriesSparkAPI() {
        JavaSparkContext sc = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("MyApp"));
        sc.setLogLevel("WARN");

        avroSchema = new Schema.Parser().parse(sc.textFile(schemaPath).first());

        data = sc.newAPIHadoopFile(
                avroPath
                , AvroKeyInputFormat.class,AvroKey.class,UserWithData.class
                ,sc.hadoopConfiguration())
                .keys()
                .map(v-> avroDecoding(v.toString()))
                .persist(StorageLevel.MEMORY_AND_DISK());
    }


    public void getPhoneFromName(String name){
        data.filter(v-> v.getName().equals(name))
                .foreach(v-> System.out.println(v.getPhone()));

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
                .union(userForChange);
        recordToAvro();
        return true;
    }



    public static UserWithData avroDecoding(String json){
        return  new Gson()
                .fromJson(json,UserWithData.class);
    }

    private void recordToAvro(){
        data.foreach(this::saveToAvro);
    }
    private GenericData.Record getRecord(UserWithData user){
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id", user.getId());
        record.put("name", user.getName());
        record.put("phone", user.getName());
        return record;
    }

    void saveToAvro(UserWithData user){
        try (DataFileWriter<GenericRecord> dataFileWriter = getFileWrite()){
            dataFileWriter.append(getRecord(user));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DataFileWriter<GenericRecord> getFileWrite() throws IOException {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(avroSchema, new File(avroPath));
        return dataFileWriter;
    }
}
