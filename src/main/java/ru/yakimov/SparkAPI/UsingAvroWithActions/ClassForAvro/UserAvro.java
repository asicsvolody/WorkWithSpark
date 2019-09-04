package ru.yakimov.SparkAPI.UsingAvroWithActions.ClassForAvro;

//import org.apache.avro.SchemaBuilder;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;

import java.io.Serializable;

public class UserAvro implements Serializable {


    private String id;

    @Nullable
    private String name;

    @Nullable
    private String phone;

    @Nullable
    private String age;

    @Nullable
    private String married;

    public UserAvro() {
    }

    public UserAvro(String id, String name, String phone, String age, String married) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.age = age;
        this.married = married;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getPhone() {
        return phone;
    }

    public String getAge() {
        return age;
    }

    public String getMarried() {
        return married;
    }
}
