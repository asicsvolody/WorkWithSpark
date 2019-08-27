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
    private final JavaSparkContext SC;
    private  final Schema SCHEMA;

    public DataUsing(String pathSchema) {
        SC = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("MyApp"));

        SCHEMA = new Schema.Parser()
                .parse( SC.textFile(pathSchema).first() );
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



    private DataFileWriter<GenericRecord> getFileWrite(String path) throws IOException {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(SCHEMA);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(SCHEMA, new File(path));
        return dataFileWriter;
    }

    private GenericData.Record getRecord(String [] jsonDataArr){
        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("id", Long.parseLong(jsonDataArr[0]));
        record.put("name", jsonDataArr[1]);
        record.put("phone", jsonDataArr[2]);
        return record;
    }

    void saveToAvro(String dirJson, String pathAvro){
        try (DataFileWriter<GenericRecord> dataFileWriter = getFileWrite(pathAvro)){

            for( String[] jsonStringArr : getListRecord(dirJson)){
                dataFileWriter.append(getRecord(jsonStringArr));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
