package ru.yakimov.SparkAPI.WorkWhithFiles;


import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.Schema;
//import org.apache.avro.SchemaBuilder;

import java.io.Serializable;


public class UserWithData implements Serializable, SchemaGiving {

    @JsonProperty("Number")
    private long id;

    @JsonProperty("First Name")
    private String name;

    @JsonProperty("Phone Number")
    private String phone;



    public UserWithData() {
    }

    public UserWithData(long id, String name, String phone) {
        this.id = id;
        this.name = name;
        this.phone = phone;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getPhone() {
        return phone;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }



    @Override
    public String toString() {
        return "UserWithData{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", phone='" + phone + '\'' +
                '}';
    }

    @Override
    public Schema createAvroSchema() {
        return null;
    }

//    @Override
//    public Schema createAvroSchema(){
//        return SchemaBuilder.record(UserWithData.class.getName())
//                .namespace(UserWithData.class.getPackageName())
//                .fields()
//                .requiredLong("id")
//                .requiredString("name")
//                .requiredString("phone")
//                .endRecord();
//    }
}