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
package sleeper.query.tracker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.model.output.ResultsOutputInfo;
import sleeper.query.tracker.exception.QueryTrackerException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.QueryProperty.QUERY_TRACKER_ITEM_TTL_IN_DAYS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;

/**
 * The query tracker updates and keeps track of the status of queries so that clients
 * can see how complete it is or if part or all of the query failed.
 */
public class DynamoDBQueryTracker implements QueryStatusReportListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBQueryTracker.class);

    public static final String DESTINATION = "DYNAMODB";
    public static final String QUERY_ID = "queryId";
    public static final String LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String LAST_KNOWN_STATE = "lastKnownState";
    public static final String RECORD_COUNT = "recordCount";
    public static final String SUB_QUERY_ID = "subQueryId";
    public static final String NON_NESTED_QUERY_PLACEHOLDER = "-";
    public static final String EXPIRY_DATE = "expiryDate";

    private final AmazonDynamoDB dynamoDB;
    private final String trackerTableName;
    private final long queryTrackerTTL;

    public DynamoDBQueryTracker(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.trackerTableName = instanceProperties.get(QUERY_TRACKER_TABLE_NAME);
        this.queryTrackerTTL = instanceProperties.getLong(QUERY_TRACKER_ITEM_TTL_IN_DAYS);
        this.dynamoDB = dynamoDB;
    }

    public DynamoDBQueryTracker(Map<String, String> destinationConfig) {
        this.trackerTableName = destinationConfig.get(QUERY_TRACKER_TABLE_NAME.getPropertyName());
        String ttl = destinationConfig.get(QUERY_TRACKER_ITEM_TTL_IN_DAYS.getPropertyName());
        this.queryTrackerTTL = Long.parseLong(ttl != null ? ttl : QUERY_TRACKER_ITEM_TTL_IN_DAYS.getDefaultValue());
        this.dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    }

    public TrackedQuery getStatus(String queryId) throws QueryTrackerException {
        return getStatus(queryId, NON_NESTED_QUERY_PLACEHOLDER);
    }

    public TrackedQuery getStatus(String queryId, String subQueryId) throws QueryTrackerException {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(trackerTableName)
                .addKeyConditionsEntry(QUERY_ID, new Condition()
                        .withAttributeValueList(new AttributeValue(queryId))
                        .withComparisonOperator(ComparisonOperator.EQ)
                )
                .addKeyConditionsEntry(SUB_QUERY_ID, new Condition()
                        .withAttributeValueList(new AttributeValue(subQueryId))
                        .withComparisonOperator(ComparisonOperator.EQ)
                )
        );

        if (result.getCount() == 0) {
            return null;
        } else if (result.getCount() > 1) {
            LOGGER.error("Multiple tracked queries returned: {}", result.getItems());
            throw new QueryTrackerException("More than one query with id " + queryId + " and subquery id "
                    + subQueryId + " was found.");
        }

        return toTrackedQuery(result.getItems().get(0));
    }

    private void updateState(String queryId, String subQueryId, QueryState state, long recordCount) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(QUERY_ID, new AttributeValue(queryId));
        key.put(SUB_QUERY_ID, new AttributeValue(subQueryId));

        Map<String, AttributeValueUpdate> valueUpdate = new HashMap<>();
        long now = System.currentTimeMillis() / 1000;
        long expiryDate = now + (3600 * 24 * queryTrackerTTL);
        valueUpdate.put(LAST_UPDATE_TIME, new AttributeValueUpdate(
                new AttributeValue().withN(String.valueOf(now)), AttributeAction.PUT));

        valueUpdate.put(EXPIRY_DATE, new AttributeValueUpdate(
                new AttributeValue().withN(String.valueOf(expiryDate)), AttributeAction.PUT));

        valueUpdate.put(RECORD_COUNT, new AttributeValueUpdate(
                new AttributeValue().withN(String.valueOf(recordCount)), AttributeAction.PUT));

        valueUpdate.put(LAST_KNOWN_STATE, new AttributeValueUpdate(
                new AttributeValue(state.name()), AttributeAction.PUT));

        dynamoDB.updateItem(new UpdateItemRequest(trackerTableName, key, valueUpdate));
    }

    private void updateState(String queryId, QueryState state, long recordCount) {
        updateState(queryId, NON_NESTED_QUERY_PLACEHOLDER, state, recordCount);
    }

    private void updateState(Query query, QueryState state) {
        this.updateState(query, state, 0);
    }

    private void updateState(Query query, QueryState state, long recordCount) {
        if (query instanceof LeafPartitionQuery) {
            LeafPartitionQuery leafPartitionQuery = (LeafPartitionQuery) query;
            updateState(query.getQueryId(), leafPartitionQuery.getSubQueryId(), state, recordCount);
            if (state.equals(QueryState.COMPLETED) || state.equals(QueryState.FAILED)) {
                updateStateOfParent(leafPartitionQuery);
            }
        } else {
            updateState(query.getQueryId(), state, recordCount);
        }
    }

    private void updateStateOfParent(LeafPartitionQuery leafPartitionQuery) {
        List<Map<String, AttributeValue>> trackedQueries = dynamoDB.query(new QueryRequest()
                .withTableName(trackerTableName)
                .addKeyConditionsEntry(QUERY_ID, new Condition()
                        .withAttributeValueList(new AttributeValue(leafPartitionQuery.getQueryId()))
                        .withComparisonOperator(ComparisonOperator.EQ)
                )
        ).getItems();

        List<TrackedQuery> children = trackedQueries.stream()
                .map(this::toTrackedQuery)
                .filter(trackedQuery -> !trackedQuery.getSubQueryId().equals(NON_NESTED_QUERY_PLACEHOLDER))
                .collect(Collectors.toList());

        QueryState parentState = getParentState(children);

        if (parentState != null) {
            long totalRecordCount = children.stream().mapToLong(query ->
                    query.getRecordCount() != null ? query.getRecordCount() : 0).sum();
            LOGGER.info("Updating state of parent to {}", parentState);
            updateState(leafPartitionQuery.getQueryId(), NON_NESTED_QUERY_PLACEHOLDER, parentState, totalRecordCount);
        }
    }

    private QueryState getParentState(List<TrackedQuery> children) {
        boolean allCompleted = true;
        boolean allSucceeded = true;
        boolean allFailed = true;
        for (TrackedQuery child : children) {
            switch (child.getLastKnownState()) {
                case FAILED:
                case PARTIALLY_FAILED:
                    allSucceeded = false;
                    break;
                case COMPLETED:
                    allFailed = false;
                    break;
                default:
                    allCompleted = false;
            }
        }

        if (allCompleted && allSucceeded) {
            return QueryState.COMPLETED;
        } else if (allCompleted && allFailed) {
            return QueryState.FAILED;
        } else if (allCompleted) {
            return QueryState.PARTIALLY_FAILED;
        } else {
            return null;
        }
    }

    private TrackedQuery toTrackedQuery(Map<String, AttributeValue> stringAttributeValueMap) {
        String id = stringAttributeValueMap.get(QUERY_ID).getS();
        Long updateTime = Long.valueOf(stringAttributeValueMap.get(LAST_UPDATE_TIME).getN());
        Long expiryDate = Long.valueOf(stringAttributeValueMap.get(EXPIRY_DATE).getN());
        Long recordCount = Long.valueOf(stringAttributeValueMap.get(RECORD_COUNT).getN());
        QueryState state = QueryState.valueOf(stringAttributeValueMap.get(LAST_KNOWN_STATE).getS());
        String subQueryId = stringAttributeValueMap.get(SUB_QUERY_ID).getS();

        return new TrackedQuery(id, subQueryId, updateTime, expiryDate, state, recordCount);
    }

    @Override
    public void queryQueued(Query query) {
        this.updateState(query, QueryState.QUEUED);
    }

    @Override
    public void queryInProgress(Query query) {
        this.updateState(query, QueryState.IN_PROGRESS);
    }

    @Override
    public void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries) {
        subQueries.forEach(subQuery -> this.updateState(subQuery, QueryState.QUEUED));
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        if (outputInfo.getError() != null) {
            if (outputInfo.getRecordCount() > 0) {
                this.updateState(query, QueryState.PARTIALLY_FAILED, outputInfo.getRecordCount());
            } else {
                this.updateState(query, QueryState.FAILED);
            }
        } else {
            this.updateState(query, QueryState.COMPLETED, outputInfo.getRecordCount());
        }
    }

    @Override
    public void queryFailed(Query query, Exception e) {
        this.updateState(query, QueryState.FAILED);
    }
}
