package ru.yakimov.SparkAPI;

import org.apache.avro.Schema;

public interface SchemaGiving {
    Schema createAvroSchema();
}
