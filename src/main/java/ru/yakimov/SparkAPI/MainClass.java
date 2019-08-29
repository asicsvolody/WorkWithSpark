package ru.yakimov.SparkAPI;


import ru.yakimov.SparkAPI.Spark.AvroInquiriesSparkAPI;
import ru.yakimov.SparkAPI.Spark.AvroInquiriesSparkSQL;

import java.io.IOException;

public class MainClass {
    public static void main(String[] args) {

        final int countJson = 150;
        final String DIR_JSON = "user";
        final String OUTPUT_FILE_PATH = "src/main/resources/User.avro";
        final String PATH_SCHEMA = "src/main/resources/UserWithData.avsc";


//        AvroFormsCreator.writeSchemaAvro(PATH_SCHEMA, new UserWithData());
//        DataUsing dataUsing = new DataUsing(PATH_SCHEMA);
//        UserWithDataJSONICsCreator.unloadingNewUsersWithDate(countJson,DIR_JSON);
//        dataUsing.saveToAvro(DIR_JSON, OUTPUT_FILE_PATH);

//        AvroInquiriesSparkSQL ai = new AvroInquiriesSparkSQL();
//        ai.getNameForId(12L).show();
//        ai.getDs().show();
//        ai.getPhoneForName("Владимир12").show();
//        ai.setPhoneForName("Владимир12","23333322");
//        ai.getUserWithName("Владимир12").show();
//        ai.setPhoneForName("Владимир12","7777777777777");
//
//
//        String[] classLinesArr = StringCompilation.printData(
//                "/java/projects/WorkWithSpark/src/main/java/ru/yakimov/SparkAPI/WorkWhithFiles/UserWithData.java");
//        StringCompilation.writeArrToClass(classLinesArr, "/java/projects/WorkWithSpark/src/main/resources/TaskCompile/UserWithData.java");
//
//        StringCompilation.compile(new File("/java/projects/WorkWithSpark/src/main/resources/TaskCompile/UserWithData.java"));


        AvroInquiriesSparkAPI api = new AvroInquiriesSparkAPI();

        api.getPhoneFromName("Владимир12");

        System.out.println(api.setPhoneForName("Владимир12", "7777777777"));
        System.out.println(api.setPhoneForName("notUser", "7777777777"));


        api.getPhoneFromName("Владимир12");




    }
}
