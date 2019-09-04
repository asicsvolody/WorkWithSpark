package ru.yakimov.SparkAPI.UniversalMethod;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import javax.tools.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UniversalDateUsing {

    public static Schema getSchemaFromAvro(String path) throws IOException {
        DatumReader<GenericRecord> datumReader =new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<GenericRecord>(
                        new File(path),datumReader);
        return dataFileReader.getSchema();
    }

    public static void saveSchemaTo(String path, Schema schema){
        File file = new File(path);
        if(!file.getParentFile().exists())
            file.getParentFile().mkdirs();

        try(FileWriter fileWriter = new FileWriter(path)){
            fileWriter.write(schema.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void compileAvro(String schemaPath, String compileTo) throws IOException {
        String command =
                "java -jar " +
                        "/Users/vladimir/.m2/repository/org/apache/avro/avro-tools/1.9.0/avro-tools-1.9.0.jar" +
                        " compile schema "
                        +schemaPath+" "+compileTo;
        Runtime rt = Runtime.getRuntime();
        rt.exec(command);
    }




}
