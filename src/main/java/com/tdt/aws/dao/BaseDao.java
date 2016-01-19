package com.tdt.aws.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.TimeZone;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * This class contains table definitions
 */
public class BaseDao {

    public static String USER_TABLE_NAME = "users";
    /**
     * column email also primary key
     */
    public static String USER_TABLE_PK_NAME = "email";
    /**
     * type String
     */
    public static String USER_TABLE_PK_TYPE = "S";
    public static String USER_TABLE_COLUMN_PASSWORD = "password";
    /**
     * type String
     */
    public static String USER_TABLE_COLUMN_PASSWORD_TYPE = "S";

    @Autowired
    private DynamoDB dynamoDB;

    Logger logger = LoggerFactory.getLogger(BaseDao.class);

    /**
     * Delete Dynamodb table
     * @param tableName name of table
     */
    public void deleteTable(String tableName) {
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
    public void createTable(String tableName,
                                   long readCapacityUnits,
                                   long writeCapacityUnits,
                                   String partitionKeyName,
                                   String partitionKeyType) {

        createTable(tableName, readCapacityUnits, writeCapacityUnits,
                partitionKeyName, partitionKeyType, null, null);
    }

    private void createTable(
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

    public DynamoDB getDynamoDB() {
        return dynamoDB;
    }
}