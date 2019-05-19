package com.amazonaws.lambda.hometask;

import com.amazonaws.lambda.hometask.service.DynamoDBService;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

import java.sql.Timestamp;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    static DynamoDBService dynamoDBService = null;

    static String DYNAMO_DB_TABLE_NAME = "table_test";
    static String FIELD_PACKAGE_ID = "packageId";
    static String FIELD_ORIGIN_TIME_STAMP = "originTimeStamp";
    static String FIELD_FILE_PATH = "filePath";
    static String FIELD_FILE_TYPE = "fileType";

    @Override
    public String handleRequest(S3Event event, Context context) {
        LambdaLogger logger = context.getLogger();
        event.getRecords().forEach(record -> {
            final String awsRegion = record.getAwsRegion();
            if (dynamoDBService == null) {
                dynamoDBService = new DynamoDBService(awsRegion);
            }

            final String eventName = record.getEventName();
            final String key = record.getS3().getObject().getKey();
            final String eTag = record.getS3().getObject().geteTag();
            final String bucket = record.getS3().getBucket().getName();
            final String filePath = bucket + '/' + key;

            logger.log("LAMBDA FileName:" + key);
            logger.log("Received event:" + eventName);

            // com.amazonaws.services.s3.model.S3Event
            if (eventName.contains("ObjectCreated")) {

                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                // Build the item
                Item item = new Item()
                        .withPrimaryKey(FIELD_PACKAGE_ID, eTag)
                        .withNumber(FIELD_ORIGIN_TIME_STAMP, timestamp.getTime())
                        .withString(FIELD_FILE_PATH, filePath)
                        .withString(FIELD_FILE_TYPE, key.substring(key.lastIndexOf(".") + 1));

                // Write the item to the table
                dynamoDBService.putItem(DYNAMO_DB_TABLE_NAME, item);

            } else if (eventName.contains("ObjectRemoved")) {
                dynamoDBService.deleteItem(DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, filePath,
                        FIELD_PACKAGE_ID, FIELD_ORIGIN_TIME_STAMP);
            }
        });

        return "ok";
    }
}