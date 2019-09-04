package ru.yakimov.SparkAPI.Spark;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import ru.yakimov.SparkAPI.WorkWhithFiles.UserWithData;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class AvroUsersSaver implements Serializable {




    private  String avroPath ;
    private  Schema avroSchema ;

    public AvroUsersSaver(String avroPath, String schemaPath) {
        this.avroPath = avroPath;
        this.avroSchema = new Schema.Parser().parse(AvroInquiriesSparkSQL.sc.textFile(schemaPath).first());;
    }

    private GenericData.Record getRecord(UserWithData user){
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id", user.getId());
        record.put("name", user.getName());
        record.put("phone", user.getPhone());
        return record;
    }

    public void saveToAvro(List<UserWithData> usersFromData){
        try (DataFileWriter<GenericRecord> dataFileWriter = getFileWrite()){
            for (UserWithData user: usersFromData ) {
                dataFileWriter.append(getRecord(user));
            }

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
