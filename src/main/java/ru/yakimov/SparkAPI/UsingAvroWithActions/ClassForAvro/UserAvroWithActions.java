package ru.yakimov.SparkAPI.UsingAvroWithActions.ClassForAvro;


import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.spark.sql.DataFrameReader;
import scala.xml.dtd.DEFAULT;

import java.io.Serializable;

public class UserAvroWithActions implements Serializable {
//    final String DEFAULT= "null";

    private String id;

    @Nullable
    private String name;

    @Nullable
    private String phone;

    @Nullable
    private String  age;

    @Nullable
    private String  married;

    @Nullable
    private String action;

    public UserAvroWithActions() {
    }

    public UserAvroWithActions(String id, String name, String phone, String age, String married, String action) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.age = age;
        this.married = married;
        this.action = action;
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

    public String getAction() {
        return action;
    }
}
