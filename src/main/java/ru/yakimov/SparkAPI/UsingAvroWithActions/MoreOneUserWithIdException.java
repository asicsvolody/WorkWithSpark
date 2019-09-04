package ru.yakimov.SparkAPI.UsingAvroWithActions;

public class MoreOneUserWithIdException extends Exception {
    public MoreOneUserWithIdException(String selection) {
        super("Not unique id: "+ selection);
    }
}
