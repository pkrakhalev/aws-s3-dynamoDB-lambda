package com.amazonaws.lambda.hometask.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.ScanExpressionSpec;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.S;

public class DynamoDBService {

    private DynamoDB dynamoDB;

    public DynamoDBService() throws IOException {
        Properties properties = new Properties();
        FileInputStream fileInputStream = new FileInputStream("src/main/resources/config.properties");
        properties.load(fileInputStream);
        String awsRegionName = properties.getProperty("awsRegionName");
        dynamoDB = getDynamoDB(awsRegionName);
    }

    public DynamoDBService(String awsRegion) {
        String awsRegionName = awsRegion;
        dynamoDB = getDynamoDB(awsRegionName);
    }

    private DynamoDB getDynamoDB(String awsRegionName) {
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withRegion(awsRegionName)
                .build();
        return new DynamoDB(dynamoDBClient);
    }

    public Table getTable(String tableName) {
        return dynamoDB.getTable(tableName);
    }

    public void putItem(String tableName, Item item) {
        Table table = dynamoDB.getTable(tableName);
        table.putItem(item);
    }

    public void deleteItem(String tableName, String field, String key, String hashKeyName, String rangeKeyName) {
        Table table = dynamoDB.getTable(tableName);

        List<Item> listItems = findItem(tableName, field, key);

        for (Item item : listItems) {
            table.deleteItem(hashKeyName, item.getString(hashKeyName),
                    rangeKeyName, item.getNumber(rangeKeyName));
        }
    }

    public List<Item> findItem(String tableName, String field, String key) {
        Table table = dynamoDB.getTable(tableName);

        // Find all items from the table
        ScanExpressionSpec xspec = new ExpressionSpecBuilder()
                .withCondition(
                        S(field).eq(key))
                .buildForScan();
        ItemCollection<ScanOutcome> scan = table.scan(xspec);

        List<Item> list = new ArrayList<>();
        scan.iterator().forEachRemaining(list::add);
        return list;
    }
}
