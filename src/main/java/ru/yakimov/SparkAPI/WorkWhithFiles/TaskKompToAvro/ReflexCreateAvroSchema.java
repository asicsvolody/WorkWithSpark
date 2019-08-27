package ru.yakimov.SparkAPI.WorkWhithFiles.TaskKompToAvro;


import org.apache.avro.Schema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class ReflexCreateAvroSchema {

    public void createSchema(File classFile, String pathTo)
            throws MalformedURLException, ClassNotFoundException, NoSuchMethodException
            , InvocationTargetException, IllegalAccessException {
        Class cl = getClass(classFile);
        Method createAvroSchema = cl.getDeclaredMethod("createAvroSchema");
        createAvroSchema.setAccessible(true);
        writeSchemaAvro(pathTo, (Schema) createAvroSchema.invoke(cl));
    }



    private void writeSchemaAvro(String path, Schema schema){
        try(FileWriter fileWriter = new FileWriter(path)){
            fileWriter.write(schema.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private Class<?> getClass(File classFile) throws ClassNotFoundException, MalformedURLException {
        return URLClassLoader.newInstance(new URL[]{classFile.getParentFile().toURL()})
                .loadClass("UserWithData");
    }


    private String getNameWithOutFormat(String fileNameWithFormat){
        char[] charArr = fileNameWithFormat.toCharArray();
        StringBuilder sb = new StringBuilder();
        for (char symbol: charArr) {
            if(symbol=='.'){
                break;
            }
            sb.append(symbol);
        }
        return sb.toString();
    }

}
