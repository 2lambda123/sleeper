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
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.RegionSerDe;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestorev2.PartitionStore;
import sleeper.statestorev2.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.statestorev2.s3.S3RevisionUtils.RevisionId;
import static sleeper.statestorev2.s3.S3StateStore.CURRENT_REVISION;
import static sleeper.statestorev2.s3.S3StateStore.CURRENT_UUID;
import static sleeper.statestorev2.s3.S3StateStore.REVISION_ID_KEY;
import static sleeper.statestorev2.s3.S3StateStore.getZeroPaddedLong;

public class S3PartitionStore implements PartitionStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3PartitionStore.class);
    public static final String CURRENT_PARTITIONS_REVISION_ID_KEY = "CURRENT_PARTITIONS_REVISION_ID_KEY";

    private final List<PrimitiveType> rowKeyTypes;
    private final AmazonDynamoDB dynamoDB;
    private final String dynamoRevisionIdTable;
    private final S3RevisionUtils s3RevisionUtils;
    private final Schema partitionSchema;
    private final Configuration conf;
    private final RegionSerDe regionSerDe;
    private final Schema tableSchema;
    private final String fs;
    private final String s3Bucket;


    private S3PartitionStore(Builder builder) {
        dynamoDB = builder.dynamoDB;
        dynamoRevisionIdTable = builder.dynamoRevisionIdTable;
        conf = builder.conf;
        tableSchema = builder.tableSchema;
        regionSerDe = new RegionSerDe(tableSchema);
        rowKeyTypes = tableSchema.getRowKeyTypes();
        fs = builder.fs;
        s3Bucket = builder.s3Bucket;
        s3RevisionUtils = new S3RevisionUtils(dynamoDB, dynamoRevisionIdTable);
        partitionSchema = initialisePartitionSchema();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
        int numberAttempts = 0;
        while (numberAttempts < 5) {
            RevisionId revisionId = s3RevisionUtils.getCurrentPartitionsRevisionId();
            String partitionsPath = getPartitionsPath(revisionId);
            Map<String, Partition> partitionIdToPartition;
            try {
                List<Partition> partitions = readPartitionsFromParquet(partitionsPath);
                LOGGER.debug("Attempt number {}: reading partition information (revisionId = {}, path = {})",
                        numberAttempts, revisionId, partitionsPath);
                partitionIdToPartition = getMapFromPartitionIdToPartition(partitions);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to read partition information; retrying");
                numberAttempts++;
                continue;
            }

            // Validate request
            validateSplitPartitionRequest(partitionIdToPartition, splitPartition, newPartition1, newPartition2);

            // Update map from partition id to partitions
            partitionIdToPartition.put(splitPartition.getId(), splitPartition);
            partitionIdToPartition.put(newPartition1.getId(), newPartition1);
            partitionIdToPartition.put(newPartition2.getId(), newPartition2);

            // Convert to list of partitions
            List<Partition> updatedPartitions = new ArrayList<>();
            for (Map.Entry<String, Partition> entry : partitionIdToPartition.entrySet()) {
                updatedPartitions.add(entry.getValue());
            }

            RevisionId nextRevisionId = s3RevisionUtils.getNextRevisionId(revisionId);
            String nextRevisionIdPath = getPartitionsPath(nextRevisionId);
            try {
                LOGGER.debug("Writing updated partition information (revisionId = {}, path = {})",
                        nextRevisionId, nextRevisionIdPath);
                writePartitionsToParquet(updatedPartitions, nextRevisionIdPath);
            } catch (IOException e) {
                LOGGER.debug("IOException thrown attempting to write partition information; retrying");
                numberAttempts++;
                continue;
            }
            try {
                s3RevisionUtils.conditionalUpdateOfPartitionRevisionId(revisionId, nextRevisionId);
                LOGGER.debug("Updated partition {}, new revision id is {}", splitPartition, nextRevisionId);
                break;
            } catch (ConditionalCheckFailedException e) {
                LOGGER.info("Attempt number {} to update partitions failed with conditional check failure, retrying ({})",
                        numberAttempts, e.getMessage());
                Path path = new Path(nextRevisionIdPath);
                try {
                    path.getFileSystem(new Configuration()).delete(path, false);
                    LOGGER.debug("Deleted file {}", path);
                } catch (IOException e2) {
                    LOGGER.debug("IOException attempting to delete file {}: {}", path, e.getMessage());
                    // Ignore as not essential to delete the file
                }
                numberAttempts++;
                try {
                    Thread.sleep((long) (Math.random() * 2000L));
                } catch (InterruptedException interruptedException) {
                    // Do nothing
                }
            }
        }
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        RevisionId revisionId = s3RevisionUtils.getCurrentPartitionsRevisionId();
        if (null == revisionId) {
            return Collections.EMPTY_LIST;
        }
        String path = getPartitionsPath(revisionId);
        try {
            return readPartitionsFromParquet(path);
        } catch (IOException e) {
            throw new StateStoreException("IOException reading all partitions from path " + path, e);
        }
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        // TODO Optimise by passing the leaf predicate down
        return getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
    }

    private void validateSplitPartitionRequest(Map<String, Partition> partitionIdToPartition,
                                               Partition splitPartition,
                                               Partition newPartition1,
                                               Partition newPartition2)
            throws StateStoreException {
        // Validate that splitPartition is there and is a leaf partition
        if (!partitionIdToPartition.containsKey(splitPartition.getId())) {
            throw new StateStoreException("splitPartition should be present");
        }
        if (!partitionIdToPartition.get(splitPartition.getId()).isLeafPartition()) {
            throw new StateStoreException("splitPartition should be a leaf partition");
        }

        // Validate that newPartition1 and newPartition2 are not already there
        if (partitionIdToPartition.containsKey(newPartition1.getId()) || partitionIdToPartition.containsKey(newPartition2.getId())) {
            throw new StateStoreException("newPartition1 and newPartition2 should not be present");
        }

        // Validate that the children of splitPartition are newPartition1 and newPartition2
        Set<String> splitPartitionChildrenIds = new HashSet<>(splitPartition.getChildPartitionIds());
        Set<String> newIds = new HashSet<>();
        newIds.add(newPartition1.getId());
        newIds.add(newPartition2.getId());
        if (!splitPartitionChildrenIds.equals(newIds)) {
            throw new StateStoreException("Children of splitPartition do not equal newPartition1 and new Partition2");
        }

        // Validate that the parent of newPartition1 and newPartition2 are correct
        if (!newPartition1.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition1 does not equal splitPartition");
        }
        if (!newPartition2.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition2 does not equal splitPartition");
        }

        // Validate that newPartition1 and newPartition2 are leaf partitions
        if (!newPartition1.isLeafPartition() || !newPartition2.isLeafPartition()) {
            throw new StateStoreException("newPartition1 and newPartition2 should be leaf partitions");
        }
    }

    private Schema initialisePartitionSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("partitionId", new StringType()))
                .valueFields(
                        new Field("leafPartition", new StringType()),
                        new Field("parentPartitionId", new StringType()),
                        new Field("childPartitionIds", new ListType(new StringType())),
                        new Field("region", new StringType()),
                        new Field("dimension", new IntType()))
                .build();
    }


    private String getPartitionsPath(RevisionId revisionId) {
        return fs + s3Bucket + "/statestore/partitions/" + revisionId.getRevision() + "-" + revisionId.getUuid() + "-partitions.parquet";
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(tableSchema, Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        if (null == partitions || partitions.isEmpty()) {
            throw new StateStoreException("At least one partition must be provided");
        }
        setPartitions(partitions);
    }

    private void setPartitions(List<Partition> partitions) throws StateStoreException {
        // Validate that there is no current revision id
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(REVISION_ID_KEY, new AttributeValue().withS(CURRENT_PARTITIONS_REVISION_ID_KEY));
        GetItemRequest getItemRequest = new GetItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withKey(item);
        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
        if (null != getItemResult.getItem()) {
            throw new StateStoreException("Dynamo should not contain current revision id; found " + item);
        }

        // Write partitions to file
        long version = 1L;
        String versionString = getZeroPaddedLong(version);
        RevisionId revisionId = new RevisionId(versionString, UUID.randomUUID().toString());
        String path = getPartitionsPath(revisionId);
        try {
            writePartitionsToParquet(partitions, path);
            LOGGER.debug("Written initial partitions file to {}", path);
        } catch (IOException e) {
            throw new StateStoreException("IOException writing partitions to file " + path, e);
        }

        // Update Dynamo
        item.put(CURRENT_REVISION, new AttributeValue().withS(revisionId.getRevision()));
        item.put(CURRENT_UUID, new AttributeValue().withS(revisionId.getUuid()));
        PutItemRequest putItemRequest = new PutItemRequest()
                .withTableName(dynamoRevisionIdTable)
                .withItem(item);
        dynamoDB.putItem(putItemRequest);
        LOGGER.debug("Put item to DynamoDB (item = {}, table = {})", item, dynamoRevisionIdTable);
    }

    private Map<String, Partition> getMapFromPartitionIdToPartition(List<Partition> partitions) throws StateStoreException {
        Map<String, Partition> partitionIdToPartition = new HashMap<>();
        for (Partition partition : partitions) {
            if (partitionIdToPartition.containsKey(partition.getId())) {
                throw new StateStoreException("Error: found two partitions with the same id ("
                        + partition + "," + partitionIdToPartition.get(partition.getId()) + ")");
            }
            partitionIdToPartition.put(partition.getId(), partition);
        }
        return partitionIdToPartition;
    }

    private void writePartitionsToParquet(List<Partition> partitions, String path) throws IOException {
        ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(path), partitionSchema, conf);

        for (Partition partition : partitions) {
            recordWriter.write(getRecordFromPartition(partition));
        }
        recordWriter.close();
        LOGGER.debug("Wrote partitions to " + path);
    }

    private List<Partition> readPartitionsFromParquet(String path) throws IOException {
        List<Partition> partitions = new ArrayList<>();
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(path), partitionSchema)
                .withConf(conf)
                .build();
        ParquetReaderIterator recordReader = new ParquetReaderIterator(reader);
        while (recordReader.hasNext()) {
            partitions.add(getPartitionFromRecord(recordReader.next()));
        }
        recordReader.close();
        return partitions;
    }

    private Record getRecordFromPartition(Partition partition) throws IOException {
        Record record = new Record();
        record.put("partitionId", partition.getId());
        record.put("leafPartition", "" + partition.isLeafPartition()); // TODO Change to boolean once boolean is a supported type
        String parentPartitionId;
        if (null == partition.getParentPartitionId()) {
            parentPartitionId = "null";
        } else {
            parentPartitionId = partition.getParentPartitionId();
        }
        record.put("parentPartitionId", parentPartitionId);
        record.put("childPartitionIds", partition.getChildPartitionIds());
        record.put("region", regionSerDe.toJson(partition.getRegion()));
        record.put("dimension", partition.getDimension());
        return record;
    }

    private Partition getPartitionFromRecord(Record record) throws IOException {
        Partition.Builder partitionBuilder = Partition.builder()
                .id((String) record.get("partitionId"))
                .leafPartition(record.get("leafPartition").equals("true"))
                .rowKeyTypes(rowKeyTypes)
                .childPartitionIds((List<String>) record.get("childPartitionIds"))
                .region(regionSerDe.fromJson((String) record.get("region")))
                .dimension((int) record.get("dimension"));
        String parentPartitionId = (String) record.get("parentPartitionId");
        if (!"null".equals(parentPartitionId)) {
            partitionBuilder.parentPartitionId(parentPartitionId);
        }
        return partitionBuilder.build();
    }

    public static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String dynamoRevisionIdTable;
        private Configuration conf;
        private Schema tableSchema;
        private String fs;
        private String s3Bucket;

        public Builder() {
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public Builder dynamoRevisionIdTable(String dynamoRevisionIdTable) {
            this.dynamoRevisionIdTable = dynamoRevisionIdTable;
            return this;
        }

        public Builder conf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder tableSchema(Schema tableSchema) {
            this.tableSchema = tableSchema;
            return this;
        }


        public Builder fs(String fs) {
            this.fs = fs;
            return this;
        }

        public Builder s3Bucket(String s3Bucket) {
            this.s3Bucket = s3Bucket;
            return this;
        }

        public S3PartitionStore build() {
            return new S3PartitionStore(this);
        }
    }
}
