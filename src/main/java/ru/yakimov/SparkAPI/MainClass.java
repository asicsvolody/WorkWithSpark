package ru.yakimov.SparkAPI;

public class MainClass {
    public static void main(String[] args) {

        final int countJson = 150;
        final String DIR_JSON = "user";
        final String OUTPUT_FILE_PATH = "src/main/resources/User.avro";
        final String PATH_SCHEMA = "src/main/resources/UserWithData.avsc";

        DataUsing dataUsing = new DataUsing(PATH_SCHEMA);

        dataUsing.unloadingNewUsersWithDate(countJson,DIR_JSON);

        dataUsing.writeSchemaAvro(PATH_SCHEMA);

        dataUsing.saveToAvro(DIR_JSON, OUTPUT_FILE_PATH);

    }
}
