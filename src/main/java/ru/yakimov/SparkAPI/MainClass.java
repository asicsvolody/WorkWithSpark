package ru.yakimov.SparkAPI;


import javassist.NotFoundException;
import org.apache.avro.Schema;
import ru.yakimov.SparkAPI.Spark.AvroInquiriesSparkAPI;
import ru.yakimov.SparkAPI.Spark.AvroInquiriesSparkSQL;
import ru.yakimov.SparkAPI.UniversalMethod.UniversalDateUsing;

import java.io.IOException;
import java.util.Arrays;

public class MainClass {
    public static void main(String[] args) {

        final int countJson = 150;
        final String DIR_JSON = "user";
        final String OUTPUT_FILE_PATH = "src/main/resources/User.avro";
        final String PATH_SCHEMA = "src/main/resources/UserWithData.avsc";



////        AvroFormsCreator.writeSchemaAvro(PATH_SCHEMA, new UserWithData());
////        DataUsing dataUsing = new DataUsing(PATH_SCHEMA);
////        UserWithDataJSONICsCreator.unloadingNewUsersWithDate(countJson,DIR_JSON);
////        dataUsing.saveToAvro(DIR_JSON, OUTPUT_FILE_PATH);
//
////        AvroInquiriesSparkSQL ai = new AvroInquiriesSparkSQL();
////        ai.getNameForId(12L).show();
////        ai.getDs().show();
////        ai.getPhoneForName("Владимир12").show();
////        ai.setPhoneForName("Владимир12","23333322");
////        ai.getUserWithName("Владимир12").show();
////        ai.setPhoneForName("Владимир12","7777777777777");
////
////
////        String[] classLinesArr = StringCompilation.printData(
////                "/java/projects/WorkWithSpark/src/main/java/ru/yakimov/SparkAPI/WorkWhithFiles/UserWithData.java");
////        StringCompilation.writeArrToClass(classLinesArr, "/java/projects/WorkWithSpark/src/main/resources/TaskCompile/UserWithData.java");
////
////        StringCompilation.compile(new File("/java/projects/WorkWithSpark/src/main/resources/TaskCompile/UserWithData.java"));
//
////
//        AvroInquiriesSparkAPI api = new AvroInquiriesSparkAPI();
////
//        try {
//            System.out.println(api.getPhoneFromName("Владимир1"));
//        } catch (NotFoundException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println(api.setPhoneForName("Владимир1", "12"));
//        System.out.println(api.setPhoneForName("notUser", "7777777777"));
//
//
//        try {
//            System.out.println(api.getPhoneFromName("Владимир1"));
//        } catch (NotFoundException e) {
//            e.printStackTrace();
//        }
////
//        AvroInquiriesSparkSQL ai = new AvroInquiriesSparkSQL();
//
//        ai.getDs().show();
//
//        System.out.println(ai.setPhoneForName("Владимир1","99999999999"));
//
//        ai.getDs().show();
//
//        String schemaPath = "/java/projects/WorkWithSpark/src/main/resources/Universal/Schema/Schema.avsc";
////        try {
////            Schema schema = UniversalDateUsing
////                    .getSchemaFromAvro("/java/projects/WorkWithSpark/src/main/resources/User.avro");
////            UniversalDateUsing.saveSchemaTo(schemaPath
////                    ,schema);
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
//
//
//        try {
//            UniversalDateUsing.compileAvro(schemaPath,"/java/projects/WorkWithSpark/src/main/resources/Universal");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }






    }
}
