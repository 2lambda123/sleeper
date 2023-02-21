/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sleeper.dynamodb.tools;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamoDBRecordBuilderIT extends DynamoDBTableTestBase {
    @Test
    public void shouldCreateRecordWithStringAttribute() {
        // Given we have a table in dynamodb that accepts strings
        createStringTable();
        // When we create a record with string attributes
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .string(TEST_VALUE, "value").build();
        // And save that record to dynamodb
        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record));
        // Then we should be able to retrieve the record from dynamodb
        ScanResult result = dynamoDBClient.scan(new ScanRequest().withTableName(TEST_TABLE_NAME));
        assertThat(result.getItems()).containsExactly(record);
    }


    @Test
    public void shouldCreateRecordWithIntAttribute() {
        // Given we have a table in dynamodb that accepts strings
        createNumericTable();
        // When we create a record with string attributes
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .number(TEST_VALUE, 123).build();
        // And save that record to dynamodb
        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record));
        // Then we should be able to retrieve the record from dynamodb
        ScanResult result = dynamoDBClient.scan(new ScanRequest().withTableName(TEST_TABLE_NAME));
        assertThat(result.getItems()).containsExactly(record);
    }

    @Test
    public void shouldCreateRecordWithLongAttribute() {
        // Given we have a table in dynamodb that accepts strings
        createNumericTable();
        // When we create a record with string attributes
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .number(TEST_VALUE, 123L).build();
        // And save that record to dynamodb
        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record));
        // Then we should be able to retrieve the record from dynamodb
        ScanResult result = dynamoDBClient.scan(new ScanRequest().withTableName(TEST_TABLE_NAME));
        assertThat(result.getItems()).containsExactly(record);
    }

    @Test
    public void shouldCreateRecordWithInstantAttribute() {
        // Given we have a table in dynamodb that accepts strings
        createNumericTable();
        // When we create a record with string attributes
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .number(TEST_VALUE, Instant.now().toEpochMilli()).build();
        // And save that record to dynamodb
        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record));
        // Then we should be able to retrieve the record from dynamodb
        ScanResult result = dynamoDBClient.scan(new ScanRequest().withTableName(TEST_TABLE_NAME));
        assertThat(result.getItems()).containsExactly(record);
    }
}