package ru.yakimov.SparkAPI;

import net.arnx.jsonic.JSON;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class UserWithDataJSONICsCreator {
    static void unloadingNewUsersWithDate(int howMany, String toDir){
        File dir = new File(toDir);
        boolean isThereDir = true;

        if(!dir.exists())
            isThereDir = dir.mkdirs();

        if(!isThereDir)
            return;

        for (int i = 0; i < howMany; i++) {
            UserWithData user = new UserWithData(1+i,
                    "Владимир"+(i+1),
                    "+7"+ (int) (Math.random() * 1000000000));
            try {
                convertToJSonic(user, dir.getPath()+"/user"+(i+1)+".json");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private static <T>void convertToJSonic(T obj, String filePath) throws IOException {
        try(FileWriter fileWriter = new FileWriter(filePath)){
            fileWriter.write(JSON.encode(obj));
        }
    }
}
