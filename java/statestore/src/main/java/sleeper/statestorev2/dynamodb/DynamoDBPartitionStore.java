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
package sleeper.statestorev2.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.IdempotentParameterMismatchException;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.RequestLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.TransactionConflictException;
import com.amazonaws.services.dynamodbv2.model.TransactionInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestorev2.PartitionStore;
import sleeper.statestorev2.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestorev2.dynamodb.DynamoDBPartitionFormat.IS_LEAF;

public class DynamoDBPartitionStore implements PartitionStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBPartitionStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String tableName;
    private final Schema schema;
    private final boolean stronglyConsistentReads;
    private final DynamoDBPartitionFormat partitionFormat;

    private DynamoDBPartitionStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        partitionFormat = new DynamoDBPartitionFormat(schema);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(
            Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
        // Validate request: splitPartition should be a non-leaf partition, its children should be newPartition1
        // and newPartition2, their parent should be splitPartition and they should be leaf partitions
        if (splitPartition.isLeafPartition()) {
            throw new StateStoreException("Split partition is a leaf partition (splitPartition = " + splitPartition + ")");
        }
        Set<String> splitPartitionChildrenIds = new HashSet<>(splitPartition.getChildPartitionIds());
        Set<String> newIds = new HashSet<>();
        newIds.add(newPartition1.getId());
        newIds.add(newPartition2.getId());
        if (!splitPartitionChildrenIds.equals(newIds)) {
            throw new StateStoreException("Children of splitPartition do not equal newPartition1 and new Partition2");
        }
        if (!newPartition1.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition1 does not equal splitPartition");
        }
        if (!newPartition2.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition2 does not equal splitPartition");
        }
        if (!newPartition1.isLeafPartition() || !newPartition2.isLeafPartition()) {
            throw new StateStoreException("newPartition1 and newPartition2 should be leaf partitions");
        }
        List<TransactWriteItem> writes = new ArrayList<>();
        Map<String, AttributeValue> item;
        try {
            item = partitionFormat.getItemFromPartition(splitPartition);
        } catch (IOException e) {
            throw new StateStoreException("IOException getting item from partition", e);
        }
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":true", new AttributeValue("true"));
        Put put = new Put()
                .withTableName(tableName)
                .withItem(item)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withConditionExpression(IS_LEAF + " = :true");
        writes.add(new TransactWriteItem().withPut(put));
        for (Partition partition : Arrays.asList(newPartition1, newPartition2)) {
            Map<String, AttributeValue> item2;
            try {
                item2 = partitionFormat.getItemFromPartition(partition);
            } catch (IOException e) {
                throw new StateStoreException("IOException getting item from partition", e);
            }
            Put put2 = new Put()
                    .withTableName(tableName)
                    .withItem(item2);
            writes.add(new TransactWriteItem().withPut(put2));
        }
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Split partition {}, capacity consumed = {}",
                    splitPartition.getId(), totalCapacity);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        try {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(tableName)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = streamPagedResults(dynamoDB, scanRequest)
                    .flatMap(result -> {
                        totalCapacity.updateAndGet(old -> old + result.getConsumedCapacity().getCapacityUnits());
                        return result.getItems().stream();
                    }).collect(Collectors.toList());
            LOGGER.debug("Scanned for all partitions, capacity consumed = {}", totalCapacity);
            List<Partition> partitionResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                partitionResults.add(partitionFormat.getPartitionFromAttributeValues(map));
            }
            return partitionResults;
        } catch (ProvisionedThroughputExceededException | ResourceNotFoundException | RequestLimitExceededException
                 | InternalServerErrorException | IOException e) {
            throw new StateStoreException("Exception querying DynamoDB", e);
        }
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        // TODO optimise by pushing the predicate down to Dynamo
        return getAllPartitions().stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        if (null == partitions || partitions.isEmpty()) {
            throw new StateStoreException("At least one partition must be provided");
        }
        for (Partition partition : partitions) {
            addPartition(partition);
            LOGGER.debug("Added partition {}", partition);
        }
    }

    private void addPartition(Partition partition) throws StateStoreException {
        try {
            Map<String, AttributeValue> map = partitionFormat.getItemFromPartition(partition);
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(map)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            LOGGER.debug("Added partition with id {}, capacity consumed = {}",
                    partition.getId(), putItemResult.getConsumedCapacity().getCapacityUnits());
        } catch (IOException | ConditionalCheckFailedException | ProvisionedThroughputExceededException
                 | ResourceNotFoundException | ItemCollectionSizeLimitExceededException
                 | TransactionConflictException | RequestLimitExceededException
                 | InternalServerErrorException e) {
            throw new StateStoreException("Exception calling putItem", e);
        }
    }

    public static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String tableName;
        private Schema schema;
        private boolean stronglyConsistentReads;

        private Builder() {
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder stronglyConsistentReads(boolean stronglyConsistentReads) {
            this.stronglyConsistentReads = stronglyConsistentReads;
            return this;
        }

        public DynamoDBPartitionStore build() {
            return new DynamoDBPartitionStore(this);
        }
    }
}
