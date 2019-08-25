package ru.yakimov.SparkAPI;

import net.arnx.jsonic.JSON;
import net.arnx.jsonic.JSONReader;
import net.arnx.jsonic.JSONWriter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.execution.columnar.compression.RunLengthEncoding;
import org.mortbay.util.ajax.JSONObjectConvertor;
import scala.util.parsing.json.JSONObject;

import java.io.*;

public class DataUsing {
    public static final String DIR_JSON = "user";
    public static final String PATH_SCHEMA = "src/main/resources/UserWhithData.avsc";
    public static final String OUTPUT_FILE_PATH = "src/main/resources/User.avro"
    public static final JavaSparkContext SC = new JavaSparkContext(
            new SparkConf().setMaster("local").setAppName("MyApp"));


    private static <T>void convertToJSonic(T obj, String filePath) throws IOException {
        try(FileWriter fileWriter = new FileWriter(filePath)){
            fileWriter.write(JSON.encode(obj));
        }
    }

    private static void unloadingNewUsersWithDate (int howMany, String toDir){
        File dir = new File(toDir);
        boolean isThereDir = true;

        if(!dir.exists())
            isThereDir = dir.mkdirs();

        if(!isThereDir)
            return;

        for (int i = 0; i < howMany; i++) {
            UserWithData user = new UserWithData(1+i,
                    "Владимир"+(i+1),
                    "+7"+ (int) (Math.random() * 1000000000),
                    "high");
            try {
                convertToJSonic(user, dir.getPath()+"/user"+(i+1)+".json");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void writeSchemaToAvro(Schema schema, String path){
        try(FileWriter fileWriter = new FileWriter(path)){
            fileWriter.write(schema.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static JavaRDD<String> getRDDJsonsString(String dir){
        return SC.textFile(dir+"/*.json");

    }


    private static Schema getSchemaString(String pathAvtoSchema){
        return new Schema.Parser()
                .parse( SC.textFile(pathAvtoSchema).first() );
    }

    private static DataFileWriter getFileWrite(Schema schema) throws IOException {

//        Schema schema = getSchemaString(PATH_SCHEMA);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, new File(OUTPUT_FILE_PATH));
        return dataFileWriter;
    }

    public static GenericData.Record getRecourd(Schema schema, String json){

        GenericData.Record record = new GenericData.Record(schema);
        record.put("user_id", Long.parseLong(json))
        return record;
    }



//    private static



//    private byte[] fromJsonToAvroByteCode(String json, String schemaStr) throws IOException {
//        InputStream input = new ArrayInputStream(json.getBytes());
//        DataInputStream dataInputStream = new DataInputStream(input);
//        Schema schema = Schema.parse(schemaStr);
//        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);
//        DatumReader<Object> reader = new GenericDatumReader<>(schema);
//        Object datum = reader.read(null, decoder);
//
//        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//
//        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
//        writer.write(datum, encoder);
//        encoder.flush();
//        return outputStream.toByteArray();
//    }










    public static void main(String[] args) {
        final int countJson = 150;

        unloadingNewUsersWithDate(countJson,DIR_JSON);

        writeSchemaToAvro(UserWithData.createAvroSchema(),PATH_SCHEMA);

//        System.out.println(getRDDSchemaString(PATH_SCHEMA));

//        for (String arg : getRDDJsonsString(DIR_JSON).collect()) {
//            System.out.println(arg);
//        }




    }


}
