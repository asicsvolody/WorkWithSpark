package ru.yakimov.SparkAPI.WorkWhithFiles;

import java.io.FileWriter;
import java.io.IOException;

public class AvroFormsCreator {

    public static<E extends SchemaGiving> void writeSchemaAvro(String path, E obj ){
        try(FileWriter fileWriter = new FileWriter(path)){
            fileWriter.write(obj.createAvroSchema().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
