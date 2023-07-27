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
package sleeper.statestorev2.s3;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static sleeper.statestore.s3.S3StateStore.CURRENT_FILES_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.CURRENT_PARTITIONS_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.CURRENT_REVISION;
import static sleeper.statestore.s3.S3StateStore.CURRENT_UUID;
import static sleeper.statestore.s3.S3StateStore.REVISION_ID_KEY;

public class S3RevisionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3RevisionUtils.class);

    private final String dynamoRevisionIdTable;
    private final AmazonDynamoDB dynamoDB;

    public S3RevisionUtils(AmazonDynamoDB dynamoDB, String dynamoRevisionIdTable) {
        this.dynamoDB = dynamoDB;
        this.dynamoRevisionIdTable = dynamoRevisionIdTable;
    }

    public RevisionId getCurrentPartitionsRevisionId() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(REVISION_ID_KEY, new AttributeValue().withS(CURRENT_PARTITIONS_REVISION_ID_KEY));
        GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withKey(key);
        GetItemResult result = dynamoDB.getItem(getItemRequest);
        if (null == result || null == result.getItem() || result.getItem().isEmpty()) {
            return null;
        }
        Map<String, AttributeValue> map = result.getItem();
        String revision = map.get(CURRENT_REVISION).getS();
        String uuid = map.get(CURRENT_UUID).getS();
        return new RevisionId(revision, uuid);
    }

    public RevisionId getCurrentFilesRevisionId() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(REVISION_ID_KEY, new AttributeValue().withS(CURRENT_FILES_REVISION_ID_KEY));
        GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withKey(key);
        GetItemResult result = dynamoDB.getItem(getItemRequest);
        if (null == result || null == result.getItem() || result.getItem().isEmpty()) {
            return null;
        }
        Map<String, AttributeValue> map = result.getItem();
        String revision = map.get(CURRENT_REVISION).getS();
        String uuid = map.get(CURRENT_UUID).getS();
        return new RevisionId(revision, uuid);
    }

    public void conditionalUpdateOfPartitionRevisionId(RevisionId currentRevisionId, RevisionId newRevisionId) {
        LOGGER.debug("Attempting conditional update of partition information from revision id {} to {}", currentRevisionId, newRevisionId);
        conditionalUpdateOfRevisionId(CURRENT_PARTITIONS_REVISION_ID_KEY, currentRevisionId, newRevisionId);
    }

    public void conditionalUpdateOfFileInfoRevisionId(RevisionId currentRevisionId, RevisionId newRevisionId) {
        LOGGER.debug("Attempting conditional update of file information from revision id {} to {}", currentRevisionId, newRevisionId);
        conditionalUpdateOfRevisionId(CURRENT_FILES_REVISION_ID_KEY, currentRevisionId, newRevisionId);
    }

    private void conditionalUpdateOfRevisionId(String revisionIdValue, RevisionId currentRevisionId, RevisionId newRevisionId) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(REVISION_ID_KEY, new AttributeValue().withS(revisionIdValue));
        item.put(CURRENT_REVISION, new AttributeValue().withS(newRevisionId.getRevision()));
        item.put(CURRENT_UUID, new AttributeValue().withS(newRevisionId.getUuid()));

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":currentrevision", new AttributeValue(currentRevisionId.getRevision()));
        expressionAttributeValues.put(":currentuuid", new AttributeValue(currentRevisionId.getUuid()));
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withItem(item)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withConditionExpression(CURRENT_REVISION + " = :currentrevision and " + CURRENT_UUID + " = :currentuuid");
        dynamoDB.putItem(putItemRequest);
    }

    public RevisionId getNextRevisionId(RevisionId currentRevisionId) {
        String revision = currentRevisionId.getRevision();
        while (revision.startsWith("0")) {
            revision = revision.substring(1);
        }
        long revisionNumber = Long.parseLong(revision);
        long nextRevisionNumber = revisionNumber + 1;
        StringBuilder nextRevision = new StringBuilder("" + nextRevisionNumber);
        while (nextRevision.length() < 12) {
            nextRevision.insert(0, "0");
        }
        return new RevisionId(nextRevision.toString(), UUID.randomUUID().toString());
    }

    public static class RevisionId {
        private final String revision;
        private final String uuid;

        RevisionId(String revision, String uuid) {
            this.revision = revision;
            this.uuid = uuid;
        }

        public String getRevision() {
            return revision;
        }

        public String getUuid() {
            return uuid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RevisionId)) {
                return false;
            }
            RevisionId that = (RevisionId) o;
            return Objects.equals(revision, that.revision) && Objects.equals(uuid, that.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(revision, uuid);
        }

        @Override
        public String toString() {
            return "RevisionId{" +
                    "revision='" + revision + '\'' +
                    ", uuid='" + uuid + '\'' +
                    '}';
        }
    }
}
