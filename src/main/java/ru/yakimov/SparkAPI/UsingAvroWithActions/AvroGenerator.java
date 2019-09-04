package ru.yakimov.SparkAPI.UsingAvroWithActions;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import ru.yakimov.SparkAPI.UsingAvroWithActions.ClassForAvro.UserAvro;
import ru.yakimov.SparkAPI.UsingAvroWithActions.ClassForAvro.UserAvroWithActions;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AvroGenerator implements Serializable {


    public List<UserAvro> getUsers(){
        List<UserAvro> userAvros = new ArrayList<>();
        userAvros.add(new UserAvro("1", "Vladimir", "+79625957309", "31", "yes"));
        userAvros.add(new UserAvro("2", "Maxim", "+798432345422", "25", "yes"));
        userAvros.add(new UserAvro("3","Irina", "+79297470477", "30", "yes"));
        userAvros.add(new UserAvro("4", "Masha", "+787653029344", "7", "no"));
        userAvros.add(new UserAvro("5","Sergei", "+79745345223", "25", "no"));
        return userAvros;
    }

    public List<UserAvroWithActions> getUsersWithActions(){
        List<UserAvroWithActions> userAvroWithAcrionsList = new ArrayList<>();
        userAvroWithAcrionsList.add(new UserAvroWithActions("6", "Anna", "+78746345323", "35", "yes", "I" ));
        userAvroWithAcrionsList.add(new UserAvroWithActions("5",null, null , null, null, "D" ));
        userAvroWithAcrionsList.add(new UserAvroWithActions("4", null, "+78746543291", null, null, "U"));
        return userAvroWithAcrionsList;
    }

    private GenericData.Record getRecordUserAvro(UserAvro user, Schema avroSchema){

        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id", user.getId());
        record.put("name", user.getName());
        record.put("phone", user.getPhone());
        record.put("age", user.getAge());
        record.put("married", user.getMarried());

        return record;
    }

    private GenericData.Record getRecordUserAvroWithActions(UserAvroWithActions user, Schema avroSchema){

        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id", user.getId());
        record.put("name", user.getName());
        record.put("phone", user.getPhone());
        record.put("age", user.getAge());
        record.put("married", user.getMarried());
        record.put("action", user.getAction());

        return record;
    }

    public void createAvroUserAvro(String path, List<UserAvro> userAvroList){
        Schema schema = ReflectData.get().getSchema(UserAvro.class);
        System.out.println(schema.toString());
        try (DataFileWriter<GenericRecord> dataFileWriter = getFileWrite(schema, path)){
            for (UserAvro user: userAvroList ) {
                dataFileWriter.append(getRecordUserAvro(user,schema));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void createAvroUserAvroWithActions(String path, List<UserAvroWithActions> userAvroList){
        Schema newSchema = ReflectData.get().getSchema(UserAvroWithActions.class);
        try (DataFileWriter<GenericRecord> dataFileWriter = getFileWrite(newSchema, path)){
            for (UserAvroWithActions user: userAvroList ) {
                dataFileWriter.append(getRecordUserAvroWithActions(user,newSchema));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    private DataFileWriter<GenericRecord> getFileWrite(Schema avroSchema, String avroPath) throws IOException {

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(avroSchema, new File(avroPath));
        return dataFileWriter;
    }

    public static void main(String[] args) {
        AvroGenerator generator = new AvroGenerator();
        generator.createAvroUserAvro(
                "src/main/resources/UsingAvroWithActions/mainAvro.avro"
                        ,generator.getUsers());
        generator.createAvroUserAvroWithActions(
                "src/main/resources/UsingAvroWithActions/NewAvro/newAvroWithActions.avro"
                        ,generator.getUsersWithActions());
    }







}
