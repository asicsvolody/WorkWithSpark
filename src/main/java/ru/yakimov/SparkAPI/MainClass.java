package ru.yakimov.SparkAPI;

public class MainClass {
    public static void main(String[] args) {

        final int countJson = 150;
        final String DIR_JSON = "user";
        final String OUTPUT_FILE_PATH = "src/main/resources/User.avro";
        final String PATH_SCHEMA = "src/main/resources/UserWithData.avsc";


        AvroFormsCreator.writeSchemaAvro(PATH_SCHEMA, new UserWithData());

        DataUsing dataUsing = new DataUsing(PATH_SCHEMA);

        UserWithDataJSONICsCreator.unloadingNewUsersWithDate(countJson,DIR_JSON);

        dataUsing.saveToAvro(DIR_JSON, OUTPUT_FILE_PATH);


    }
}
