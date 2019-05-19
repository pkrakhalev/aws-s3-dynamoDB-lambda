package com.amazonaws.lambda.hometask;

import com.amazonaws.lambda.hometask.service.DynamoDBService;
import com.amazonaws.lambda.hometask.service.S3Service;
import com.amazonaws.services.dynamodbv2.document.Item;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static com.amazonaws.lambda.hometask.LambdaFunctionHandler.DYNAMO_DB_TABLE_NAME;
import static com.amazonaws.lambda.hometask.LambdaFunctionHandler.FIELD_FILE_PATH;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaFunctionHandlerIntegrationTest {

    private static S3Service s3Service;
    private static DynamoDBService dynamoDBService;

    private static Stream<Arguments> getTestFiles () {
        return Stream.of(Arguments.of(new File("src/test/resources/test.txt")));
    }

    @BeforeAll
    static void setUp() throws IOException {
        s3Service = new S3Service();
        dynamoDBService = new DynamoDBService();
    }

    @ParameterizedTest
    @MethodSource("getTestFiles")
    void uploadFileToS3(File file) throws IOException, InterruptedException {
        assertTrue(file.exists(), "Test file is not exist");

        final String fileName = s3Service.saveFileToS3(file);
        assertTrue(s3Service.doesFileExist(fileName), "The file should be in S3");

        final String fullName = s3Service.getS3BucketName() + '/' + fileName;

        boolean isEquals = waitItemsCountEquals(1, DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, fullName);
        assertTrue(isEquals, "DynamoDB should contain the record");
    }

    @ParameterizedTest
    @MethodSource("getTestFiles")
    void uploadAndDeleteFileFromS3(File file) throws IOException, InterruptedException {
        assertTrue(file.exists(), "Test file is not exist");

        final String fileName = s3Service.saveFileToS3(file);
        assertTrue(s3Service.doesFileExist(fileName), "The file should be in S3");

        final String fullName = s3Service.getS3BucketName() + '/' + fileName;
        boolean isEquals = waitItemsCountEquals(1, DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, fullName);
        assertTrue(isEquals, "DynamoDB should contain the record");

        s3Service.deleteFileFromS3(fileName);
        assertFalse(s3Service.doesFileExist(fileName), "The file should not be in S3");

        isEquals = waitItemsCountEquals(0, DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, fullName);
        assertTrue(isEquals, "DynamoDB should contain the record");
    }

    // sometimes AWS takes a time to update data base records after S3 update and lambda execution
    private boolean waitItemsCountEquals(int expectedSize, String tableName, String field, String key) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            List<Item> list = dynamoDBService.findItem(tableName, field, key);
            if (list.size() == expectedSize) {
                return true;
            } else {
                Thread.sleep(1000);
            }
        }
        return false;
    }
}
