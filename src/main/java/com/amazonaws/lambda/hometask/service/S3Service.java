package com.amazonaws.lambda.hometask.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public class S3Service {

    private final String mAWSRegionName;
    private final String mS3Bucket;
    private AmazonS3 s3Client = null;
    private TransferManager transferManager = null;

    public S3Service() throws IOException {
        Properties properties = new Properties();
        FileInputStream fileInputStream = new FileInputStream("src/main/resources/config.properties");
        properties.load(fileInputStream);

        this.mAWSRegionName = properties.getProperty("awsRegionName");
        this.mS3Bucket = properties.getProperty("bucketName");
    }

    private AmazonS3 getS3Client() {
        if (s3Client == null) {
            s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(mAWSRegionName)
                    .build();
        }
        return s3Client;
    }

    private TransferManager getTransferManager() {
        if (transferManager == null) {
            transferManager = TransferManagerBuilder.standard().withS3Client(getS3Client()).build();
        }
        return transferManager;
    }

    public String saveFileToS3(File file) throws FileNotFoundException {
        // Put file in format "UUID.ext"
        String randomUUIDString = UUID.randomUUID().toString();
        String extension = getFileExtension(file.getName()).orElseThrow(FileNotFoundException::new);
        String fileName = randomUUIDString + extension;

        Upload upload = getTransferManager().upload(mS3Bucket, fileName, file);
        XferMgrProgress.showTransferProgress(upload);
        XferMgrProgress.waitForCompletion(upload);

        return fileName;
    }

    public void deleteFileFromS3(String fileName) {
        System.out.println("Delete file: " + fileName);
        getS3Client().deleteObject(new DeleteObjectRequest(mS3Bucket, fileName));
        System.out.println("Deleted");
    }

    private Optional<String> getFileExtension(String filename) {
        return Optional.ofNullable(filename)
                .filter(f -> f.contains("."))
                .map(f -> f.substring(filename.lastIndexOf('.')));
    }

    public boolean doesFileExist(String fileName) {
        return getS3Client().doesObjectExist(mS3Bucket, fileName);
    }

    public String getS3BucketName() {
        return mS3Bucket;
    }
}
