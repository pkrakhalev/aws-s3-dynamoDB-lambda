package com.amazonaws.lambda.hometask;

import com.amazonaws.lambda.hometask.service.DynamoDBService;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import static com.amazonaws.lambda.hometask.LambdaFunctionHandler.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamoDBServiceUnitTest {

    @BeforeAll
    static void setUp() throws IOException {
        dynamoDBService = new DynamoDBService();
    }

    @Test
    void addItemToDateBaseDirectly() {
        Table table = dynamoDBService.getTable(DYNAMO_DB_TABLE_NAME);

        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();

        final String packageId = "1";
        final String testPath = "testPath";
        final String testType = "testType";

        // Build the item
        Item item = new Item()
                .withPrimaryKey(FIELD_PACKAGE_ID, packageId)
                .withNumber(FIELD_ORIGIN_TIME_STAMP, timestamp)
                .withString(FIELD_FILE_PATH, testPath)
                .withString(FIELD_FILE_TYPE, testType);

        // Write the item to the table
        dynamoDBService.putItem(DYNAMO_DB_TABLE_NAME, item);

        // Find all items from the table
        List<Item> list = dynamoDBService.findItem(DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, testPath);
        assertEquals(1, list.size(), "DynamoDB should have the record");

        Item scanItem = list.get(0);
        assertAll("Return record should be equals put record",
                () -> assertEquals(packageId, scanItem.getString(FIELD_PACKAGE_ID)),
                () -> assertEquals(timestamp, scanItem.getNumber(FIELD_ORIGIN_TIME_STAMP).longValue()),
                () -> assertEquals(testPath, scanItem.getString(FIELD_FILE_PATH)),
                () -> assertEquals(testType, scanItem.getString(FIELD_FILE_TYPE))
        );

        // Delete the item from the table
        dynamoDBService.deleteItem(DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, testPath,
                FIELD_PACKAGE_ID, FIELD_ORIGIN_TIME_STAMP);

        list = dynamoDBService.findItem(DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, testPath);
        assertEquals(0, list.size(), "The record should be removed from DynamoDB");
    }
}
