package ru.yakimov.SparkAPI.UsingAvroWithActions;

public class NotDirectoryException extends Exception {
    public NotDirectoryException(String directuryPath) {
        super("it is not directory: "+directuryPath);
    }
}
