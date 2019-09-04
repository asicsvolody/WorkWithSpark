package ru.yakimov.SparkAPI.UsingAvroWithActions.ClassForAvro;

import java.io.Serializable;

public enum Actions implements Serializable {
    I( "INSERT"),
    U("UPDATE"),
    D("DELETE");


    private String title;

    Actions(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    @Override
    public String toString() {
        return "Actions{" +
                "title='" + title + '\'' +
                '}';
    }
}
