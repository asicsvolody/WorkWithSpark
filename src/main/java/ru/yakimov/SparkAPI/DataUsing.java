package ru.yakimov.SparkAPI;

import net.arnx.jsonic.JSON;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.spark.sql.execution.columnar.compression.RunLengthEncoding;

import java.io.*;

public class DataUsing {


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
        unloadingNewUsersWithDate(150,"user");
        writeSchemaToAvro(UserWithData.createAvroSchema(),"src/main/resources/UserWhithData.avsc");


    }


}
