package com.tdt.aws.util;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

/**
 * Created by khoi on 1/17/2016.
 */

/**
 * static implementation of BaseDao
 */
@Component
public class DynamoDBUtils {

    static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new EnvironmentVariableCredentialsProvider()));

    static SimpleDateFormat dateFormatter = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static Logger logger = LoggerFactory.getLogger(DynamoDBUtils.class);

    /**
     * Delete Dynamodb table
     * @param tableName name of table
     */
    public static void deleteTable(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        try {
                logger.info("Issuing DeleteTable request for " + tableName);
                table.delete();
                logger.info("Waiting for " + tableName
                        + " to be deleted...this may take a while...");
                table.waitForDelete();

        } catch (Exception e) {
            logger.info("DeleteTable request failed for " + tableName);
            logger.error(e.getMessage());
        }
    }

    /**
     * Create Dynamodb table
     *
     * @param tableName          name of table
     * @param readCapacityUnits  reading capacity thoughput
     * @param writeCapacityUnits writing capacity thoughput
     * @param partitionKeyName   primary key name
     * @param partitionKeyType   primary key type
     */
    public static void createTable(String tableName,
                                   long readCapacityUnits,
                                   long writeCapacityUnits,
                                   String partitionKeyName,
                                   String partitionKeyType) {

        createTable(tableName, readCapacityUnits, writeCapacityUnits,
                partitionKeyName, partitionKeyType, null, null);
    }

    private static void createTable(
            String tableName, long readCapacityUnits, long writeCapacityUnits,
            String partitionKeyName, String partitionKeyType,
            String sortKeyName, String sortKeyType) {

        try {

            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement()
                    .withAttributeName(partitionKeyName)
                    .withKeyType(KeyType.HASH)); //Partition key

            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition()
                    .withAttributeName(partitionKeyName)
                    .withAttributeType(partitionKeyType));

            if (sortKeyName != null) {
                keySchema.add(new KeySchemaElement()
                        .withAttributeName(sortKeyName)
                        .withKeyType(KeyType.RANGE)); //Sort key
                attributeDefinitions.add(new AttributeDefinition()
                        .withAttributeName(sortKeyName)
                        .withAttributeType(sortKeyType));
            }

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(readCapacityUnits)
                            .withWriteCapacityUnits(writeCapacityUnits));

            request.setAttributeDefinitions(attributeDefinitions);

            System.out.println("Issuing CreateTable request for " + tableName);
            Table table = dynamoDB.createTable(request);
            System.out.println("Waiting for " + tableName
                    + " to be created...this may take a while...");
            table.waitForActive();

        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    public static DynamoDB getDynamoDB() {
        return dynamoDB;
    }
}
