package com.tdt.aws.dao;

import com.amazonaws.Request;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.transform.GetItemRequestMarshaller;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by khoi on 1/17/2016.
 */
@Component
public class UserDAO extends BaseDao {

    private Logger logger = LoggerFactory.getLogger(UserDAO.class);

    public boolean checkLogin(String email, String password) throws JSONException {

        DynamoDB db = getDynamoDB();
        Table table = db.getTable(USER_TABLE_NAME);
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(USER_TABLE_PK_NAME, new AttributeValue().withS(email));
        GetItemRequest getItemRequest = new GetItemRequest().withKey(key).withTableName(USER_TABLE_NAME);

        logger.info("This is JSON body request to get data from DB by primary key : {} \n", new JSONObject(getItemRequest.toString()).toString());
        logger.info("loading user from db...");
        Item data = table.getItem(new PrimaryKey().addComponent(USER_TABLE_PK_NAME, email));
        logger.info("This is JSON Object");
        logger.info(data.toJSON());
        logger.info("checking...");
        return email.equals(data.get(USER_TABLE_PK_NAME)) && password.equals(data.get(USER_TABLE_COLUMN_PASSWORD));
    }

    public void createUser(String email, String password) {

        Table table = getDynamoDB().getTable(USER_TABLE_NAME);

        Item item = new Item()
                .withPrimaryKey(USER_TABLE_PK_NAME, email)
                .withString(USER_TABLE_COLUMN_PASSWORD, password);
        table.putItem(item);
    }
}
