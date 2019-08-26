package ru.yakimov.SparkAPI;

import net.arnx.jsonic.JSON;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

public class DataUsing implements Serializable {
    private static final String DIR_JSON = "user";
    private static final String OUTPUT_FILE_PATH = "src/main/resources/User.avro";
    private static final String PATH_SCHEMA = "src/main/resources/UserWhithData.avsc";
    private static final JavaSparkContext SC = new JavaSparkContext(
            new SparkConf().setMaster("local").setAppName("MyApp"));
    private static final Schema SCHEMA = new Schema.Parser()
            .parse( SC.textFile(PATH_SCHEMA).first() );


    private static <T>void convertToJSonic(T obj, String filePath) throws IOException {
        try(FileWriter fileWriter = new FileWriter(filePath)){
            fileWriter.write(JSON.encode(obj));
        }
    }

    private void unloadingNewUsersWithDate (int howMany, String toDir){
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

    private void writeSchemaAvro(String path){
        try(FileWriter fileWriter = new FileWriter(path)){
            fileWriter.write(SCHEMA.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<String[]> getListRecord(String dir){
        return SC.textFile(dir+"/*.json")
                .map(DataUsing::dataJson)
                .collect();
    }

    private static String[] dataJson(String json){
        char[] charJson = json.toCharArray();
        for (int i = 0; i < charJson.length; i++) {
            switch (charJson[i]){
                case '{':
                case '}':
                case '"':
                case ':':
                case ',':
                    charJson[i] = ' ';

            }
        }
        return removeSpaces(new String(charJson).split(" "));
    }

    private static String [] removeSpaces(String[] strArr){
        ArrayList<String> arrayList = new ArrayList<>();
        int count = 1;
        for (String str : strArr) {
            if(!str.equals("") && count++%2 ==0)
                arrayList.add(str);
        }
        return  arrayList.toArray(new String[0]);
    }



    private DataFileWriter<GenericRecord> getFileWrite() throws IOException {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(SCHEMA);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(SCHEMA, new File(OUTPUT_FILE_PATH));
        return dataFileWriter;
    }

    private GenericData.Record getRecord(String [] jsonDataArr){
        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("id", Long.parseLong(jsonDataArr[0]));
        record.put("name", jsonDataArr[1]);
        record.put("phone", jsonDataArr[2]);
        return record;
    }

    private void saveToAvro(){
        try (DataFileWriter<GenericRecord> dataFileWriter = getFileWrite()){

            for( String[] jsonStringArr : getListRecord(DIR_JSON)){
                dataFileWriter.append(getRecord(jsonStringArr));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {


        final int countJson = 150;

        DataUsing dataUsing = new DataUsing();



        dataUsing.unloadingNewUsersWithDate(countJson,DIR_JSON);

        dataUsing.writeSchemaAvro(PATH_SCHEMA);

        dataUsing.saveToAvro();





    }


}
