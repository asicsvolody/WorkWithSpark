package ru.yakimov.SparkAPI.UsingAvroWithActions;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SparkDataWorker {
    private final String primaryKayFildName = "id";
    private String pathSaveTo ;
    private Dataset<Row> data;
    private StructType structType;
    private SparkSession spark;

    public SparkDataWorker(String pathSaveTo ) {
        this.pathSaveTo = pathSaveTo;
        spark = SparkSession.builder()
                .appName("Universal App")
                .config("spark.master", "local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        data = spark
                .read()
                .format("avro")
                .load(pathSaveTo)
                .persist(StorageLevel.MEMORY_AND_DISK());
        structType = data.first().schema();
        save();

        data.show();
    }

    public void save(){
        File file = new File(pathSaveTo);
        if(file.exists()) {
            try {
                FileUtils.deleteDirectory(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        data.write().format("avro").save(pathSaveTo);
    }


    private void usingNewDataFromDir(String dirPath) throws FileNotFoundException, NotDirectoryException {
        Dataset<Row> newData = null;
        List<Row> newRows = new ArrayList<>();

        newData = getDataFromDirectory(dirPath);

        newData.show();

        List<Row> rowsForUpdate = newData.collectAsList();

        for (Row newDataRow : rowsForUpdate) {
            switch (getDataFromFild(newDataRow , "action")){
                case "D" :
                    deleteThisLine(getPrimaryValue(newDataRow));
                    break;
                case "I" :

                    try {
                        newRows.add(insertRow(newDataRow));

                    } catch (MoreOneUserWithIdException e) {
                        e.printStackTrace();
                    }

                    break;
                case "U" :
                    try {
                        newRows.add(updateRow(newDataRow));
                        deleteThisLine(getPrimaryValue(newDataRow));

                    } catch (MoreOneUserWithIdException e) {
                        e.printStackTrace();
                    }
                    break;
            }
        }

        addToData(newRows);
        data.show();
        save();

    }

    private Row insertRow(Row row) throws MoreOneUserWithIdException {
        if(isLineWithPrimaryKay(getPrimaryValue(row))){
            throw new MoreOneUserWithIdException(getPrimaryValue(row)+ " is used");
        }
        return getNewRow(row);

    }

    private String getPrimaryValue(Row row){
        return row
                .getString(row
                        .schema()
                        .fieldIndex(primaryKayFildName));
    }

    private void addToData(List<Row> newRows) {
        data = spark
                .createDataFrame(newRows, structType)
                .union(data)
                .sort(primaryKayFildName)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }

    private Row createRowWithCentralSchema(String[] data){
        Row row = RowFactory.create(data);
        return spark.
                createDataFrame(Collections.singletonList(row), structType)
                .first();
    }

    public Dataset<Row> getDataFromDirectory (String dirPath) throws NotDirectoryException, FileNotFoundException {

        File dir = new File(dirPath);
        if(!dir.exists())
            throw new FileNotFoundException();
        if(!dir.isDirectory())
            throw new NotDirectoryException(dirPath);

        return spark.read().format("avro").load(dirPath+"/*.avro");

    }

    private Row updateRow(Row updDataRow) throws MoreOneUserWithIdException {

        Row resRow = getNewRow(updDataRow);

        Row updatingRow = getRowForPrimaryKey(getPrimaryValue(updDataRow));

        if(updatingRow == null) {
            return resRow;
        }

        String [] newData = getNewDataFromTwoRpws(resRow, updatingRow);

        return RowFactory.create(newData);

    }

    private String[] getNewDataFromTwoRpws(Row newRow, Row oldRow) {
        String [] resData = oldRow.schema().fieldNames();
        for (int i = 0; i < resData.length; i++) {
            String fildData = getDataFromFild(newRow , resData[i]);
            resData[i] = (fildData == null)
                    ? getDataFromFild(oldRow , resData[i])
                    : fildData;
        }

        return resData;

    }

    private String getDataFromFild(Row row, String fieldName) {
        int index = row.schema().fieldIndex(fieldName);
        return row.getString(index);
    }

    private Row getRowForPrimaryKey(String primaryValue) throws MoreOneUserWithIdException {
        data.createOrReplaceTempView("users");

        Dataset<Row> oneUser= spark.sql(String.format("SELECT * FROM users WHERE %s = %s", primaryKayFildName,primaryValue));
        if(oneUser.count() == 0)
            return null;
        if(oneUser.count() >1)
            throw new MoreOneUserWithIdException(primaryKayFildName+ " = "+ primaryValue);
        return oneUser.first();
    }


    private void deleteThisLine(String value){
        data.createOrReplaceTempView("users");
        data = spark.sql(String.format("SELECT * FROM users WHERE %s != %s",primaryKayFildName, value)).persist(StorageLevel.MEMORY_AND_DISK());
    }

    private Row getNewRow(Row dataRow) {

        String [] newRowData = data.schema().fieldNames();
        for (int i = 0; i <newRowData.length ; i++) {
            newRowData[i] = (isFieldWithName(newRowData[i], dataRow))
                    ? getDataFromFild(dataRow, newRowData[i])
                    : null;
        }

        return createRowWithCentralSchema(newRowData);
    }

    private boolean isLineWithPrimaryKay(String primaryValue) throws MoreOneUserWithIdException {
        return  getRowForPrimaryKey(primaryValue) != null;
    }

    private boolean isFieldWithName(String fieldName, Row row) {
        return row.schema().fieldIndex(fieldName)>= 0;
    }


    public static void main(String[] args) {
        SparkDataWorker dataWorker = new SparkDataWorker(
                "/java/projects/WorkWithSpark/src/main/resources/UsingAvroWithActions/Storage/mainAvro.avro");
        try {
            dataWorker.usingNewDataFromDir(
                    "/java/projects/WorkWithSpark/src/main/resources/UsingAvroWithActions/NewAvro");
        } catch (FileNotFoundException | NotDirectoryException e) {
            e.printStackTrace();
        }

    }

}
