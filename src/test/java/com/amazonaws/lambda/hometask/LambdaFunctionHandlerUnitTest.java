package com.amazonaws.lambda.hometask;

import com.amazonaws.lambda.hometask.service.DynamoDBService;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.amazonaws.lambda.hometask.LambdaFunctionHandler.*;
import static org.mockito.Mockito.*;

class LambdaFunctionHandlerUnitTest {

    private ObjectMapper mapper;
    private Context context;
    private LambdaLogger lambdaLogger;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        lambdaLogger = mock(LambdaLogger.class);
        context = mock(Context.class);
        when(context.getLogger()).thenReturn(lambdaLogger);
        dynamoDBService = mock(DynamoDBService.class);
    }

    @Test
    void testHandleObjectCreatedRequest() throws IOException {
        S3EventNotification s3EventNotification = mapper.
                readValue(this.getClass().getResource("/s3-event.put.json"), S3EventNotification.class);
        S3Event s3Event = new S3Event(s3EventNotification.getRecords());

        LambdaFunctionHandler s3EventHandler = new LambdaFunctionHandler();

        Assertions.assertEquals("ok", s3EventHandler.handleRequest(s3Event, context));

        verify(lambdaLogger).log("LAMBDA FileName:test2.xml");
        verify(lambdaLogger).log("Received event:ObjectCreated:Put");
        verifyNoMoreInteractions(lambdaLogger);

        verify(dynamoDBService).putItem(anyString(), any(Item.class));
        verifyNoMoreInteractions(dynamoDBService);
    }

    @Test
    void testHandleObjectRemovedRequest() throws IOException {
        S3EventNotification s3EventNotification = mapper.
                readValue(this.getClass().getResource("/s3-event.delete.json"), S3EventNotification.class);
        S3Event s3Event = new S3Event(s3EventNotification.getRecords());

        LambdaFunctionHandler s3EventHandler = new LambdaFunctionHandler();
        Assertions.assertEquals("ok", s3EventHandler.handleRequest(s3Event, context));

        verify(lambdaLogger).log("LAMBDA FileName:test.xml");
        verify(lambdaLogger).log("Received event:ObjectRemoved:Delete");
        verifyNoMoreInteractions(lambdaLogger);

        verify(dynamoDBService).deleteItem(DYNAMO_DB_TABLE_NAME, FIELD_FILE_PATH, "example-bucket/test.xml",
                FIELD_PACKAGE_ID, FIELD_ORIGIN_TIME_STAMP);
        verifyNoMoreInteractions(dynamoDBService);
    }
}
