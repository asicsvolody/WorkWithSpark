package ru.yakimov.SparkAPI;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UserWithData {

    @JsonProperty("Number")
    private long id;

    @JsonProperty("First Name")
    private String name;

    @JsonProperty("Phone Number")
    private String phone;

    @JsonIgnore
    private String level;

    public UserWithData() {
    }

    public UserWithData(long id, String name, String phone, String level) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.level = level;
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

    public String getLevel() {
        return level;
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

    public void setLevel(String level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return "UserWithData{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", phone='" + phone + '\'' +
                ", level='" + level + '\'' +
                '}';
    }
}
