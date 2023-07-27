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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;
import sleeper.statestorev2.FileInfo;
import sleeper.statestorev2.FileInfo.FileStatus;
import sleeper.statestorev2.FileInfoFactory;
import sleeper.statestorev2.FileInfoStore;
import sleeper.statestorev2.StateStore;
import sleeper.statestorev2.StateStoreException;
import sleeper.statestorev2.inmemory.InMemoryFileInfoStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.statestorev2.FileInfo.FileStatus.ACTIVE;
import static sleeper.statestorev2.FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING;

@Testcontainers
public class DynamoDBStateStoreIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @Container
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, DYNAMO_PORT, AmazonDynamoDBClientBuilder.standard());
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions,
                                             int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        String id = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(id, schema, garbageCollectorDelayBeforeDeletionInMinutes, dynamoDBClient);
        DynamoDBStateStore stateStore = dynamoDBStateStoreCreator.create();
        stateStore.initialise(partitions);
        return stateStore;
    }

    private DynamoDBStateStore getStateStore(Schema schema,
                                             List<Partition> partitions) throws StateStoreException {
        return getStateStore(schema, partitions, 0);
    }

    private DynamoDBStateStore getStateStoreFromSplitPoints(Schema schema, List<Object> splitPoints) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, splitPoints).construct(), 0);
    }

    private DynamoDBStateStore getStateStore(Schema schema, int garbageCollectorDelayBeforeDeletionInMinutes) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct(), garbageCollectorDelayBeforeDeletionInMinutes);
    }

    private DynamoDBStateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStoreFromSplitPoints(schema, Collections.EMPTY_LIST);
    }

    public static Schema schemaWithSingleRowKeyType(PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field("key", type)).build();
    }

    private Schema schemaWithTwoRowKeyTypes(PrimitiveType type1, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(new Field("key1", type1), new Field("key2", type2)).build();
    }

    public static Schema schemaWithKeyAndValueWithTypes(PrimitiveType keyType, Type valueType) {
        return Schema.builder()
                .rowKeyFields(new Field("key", keyType))
                .valueFields(new Field("value", valueType))
                .build();
    }

    @Test
    public void shouldReturnCorrectFileInfoForLongRowKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore store = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("abc")
                .fileStatus(FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        store.addFile(fileInfo);

        // Then
        assertThat(store.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.FILE_IN_PARTITION);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(1L));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(10L));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getFileLifecycleList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(1L));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(10L));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getReadyForGCFiles()).isExhausted();
        assertThat(store.getPartitionToFileInPartitionMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactly("abc"));
    }

    @Test
    public void shouldReturnCorrectFileInfoForByteArrayKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        StateStore store = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType())
                .filename("abc")
                .fileStatus(FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(new byte[]{1}))
                .maxRowKey(Key.create(new byte[]{10}))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        store.addFile(fileInfo);

        // Then
        assertThat(store.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.FILE_IN_PARTITION);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey().size()).isOne();
            assertThat((byte[]) found.getMinRowKey().get(0)).containsExactly(new byte[]{1});
            assertThat(found.getMaxRowKey().size()).isOne();
            assertThat((byte[]) found.getMaxRowKey().get(0)).containsExactly(new byte[]{10});
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getFileLifecycleList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey().size()).isOne();
            assertThat((byte[]) found.getMinRowKey().get(0)).containsExactly(new byte[]{1});
            assertThat(found.getMaxRowKey().size()).isOne();
            assertThat((byte[]) found.getMaxRowKey().get(0)).containsExactly(new byte[]{10});
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getReadyForGCFiles()).isExhausted();
        assertThat(store.getPartitionToFileInPartitionMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactly("abc"));
    }

    @Test
    public void shouldReturnCorrectFileInfoFor2DimensionalByteArrayKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        StateStore store = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType(), new ByteArrayType())
                .filename("abc")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("root")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(Arrays.asList(new byte[]{1}, new byte[]{2})))
                .maxRowKey(Key.create(Arrays.asList(new byte[]{10}, new byte[]{11})))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        store.addFile(fileInfo);

        // Then
        assertThat(store.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType(), new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.FILE_IN_PARTITION);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey().size()).isEqualTo(2);
            assertThat((byte[]) found.getMinRowKey().get(0)).containsExactly(new byte[]{1});
            assertThat((byte[]) found.getMinRowKey().get(1)).containsExactly(new byte[]{2});
            assertThat(found.getMaxRowKey().size()).isEqualTo(2);
            assertThat((byte[]) found.getMaxRowKey().get(0)).containsExactly(new byte[]{10});
            assertThat((byte[]) found.getMaxRowKey().get(1)).containsExactly(new byte[]{11});
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getFileLifecycleList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new ByteArrayType(), new ByteArrayType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey().size()).isEqualTo(2);
            assertThat((byte[]) found.getMinRowKey().get(0)).containsExactly(new byte[]{1});
            assertThat((byte[]) found.getMinRowKey().get(1)).containsExactly(new byte[]{2});
            assertThat(found.getMaxRowKey().size()).isEqualTo(2);
            assertThat((byte[]) found.getMaxRowKey().get(0)).containsExactly(new byte[]{10});
            assertThat((byte[]) found.getMaxRowKey().get(1)).containsExactly(new byte[]{11});
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getReadyForGCFiles()).isExhausted();
        assertThat(store.getPartitionToFileInPartitionMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactly("abc"));
    }

    @Test
    public void shouldReturnCorrectFileInfoForMultidimensionalRowKey() throws StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new LongType(), new StringType());
        StateStore store = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename("abc")
                .fileStatus(FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(Arrays.asList(1L, "Z")))
                .maxRowKey(Key.create(Arrays.asList(10L, "A")))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When
        store.addFile(fileInfo);

        // Then
        assertThat(store.getFileInPartitionList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType(), new StringType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.FILE_IN_PARTITION);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(Arrays.asList(1L, "Z")));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(Arrays.asList(10L, "A")));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getFileLifecycleList()).singleElement().satisfies(found -> {
            assertThat(found.getRowKeyTypes()).containsExactly(new LongType(), new StringType());
            assertThat(found.getFilename()).isEqualTo("abc");
            assertThat(found.getFileStatus()).isEqualTo(FileStatus.ACTIVE);
            assertThat(found.getPartitionId()).isEqualTo("root");
            assertThat(found.getNumberOfRecords()).isEqualTo(1000L);
            assertThat(found.getMinRowKey()).isEqualTo(Key.create(Arrays.asList(1L, "Z")));
            assertThat(found.getMaxRowKey()).isEqualTo(Key.create(Arrays.asList(10L, "A")));
            assertThat(found.getLastStateStoreUpdateTime().longValue()).isEqualTo(1_000_000L);
        });
        assertThat(store.getReadyForGCFiles()).isExhausted();
        assertThat(store.getPartitionToFileInPartitionMap())
                .containsOnlyKeys("root")
                .hasEntrySatisfying("root", files ->
                        assertThat(files).containsExactly("abc"));
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingFilename() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .fileStatus(FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When / Then
        assertThatThrownBy(() -> dynamoDBStateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExceptionThrownWhenAddingFileInfoWithMissingPartition() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("abc")
                .fileStatus(FileStatus.ACTIVE)
                .numberOfRecords(1000L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();

        // When / Then
        assertThatThrownBy(() -> dynamoDBStateStore.addFile(fileInfo))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldAtomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToRemoveFileInPartitionEntries = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileStatus.FILE_IN_PARTITION)
                    .partitionId("7")
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            filesToRemoveFileInPartitionEntries.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        FileInfo newFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-new")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .lastStateStoreUpdateTime(10_000_000L)
                .build();

        // When
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(filesToRemoveFileInPartitionEntries, newFileInfo);

        // Then
        assertThat(dynamoDBStateStore.getFileInPartitionList()).containsExactly(newFileInfo);
        assertThat(dynamoDBStateStore.getFileLifecycleList()).containsExactlyInAnyOrder(
                filesToRemoveFileInPartitionEntries.get(0).cloneWithStatus(FileStatus.ACTIVE),
                filesToRemoveFileInPartitionEntries.get(1).cloneWithStatus(FileStatus.ACTIVE),
                filesToRemoveFileInPartitionEntries.get(2).cloneWithStatus(FileStatus.ACTIVE),
                filesToRemoveFileInPartitionEntries.get(3).cloneWithStatus(FileStatus.ACTIVE),
                newFileInfo.cloneWithStatus(FileStatus.ACTIVE)
        );
    }

    @Test
    public void shouldatomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFilesForSplittingJob() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> filesToRemoveFileInPartitionEntries = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileStatus.FILE_IN_PARTITION)
                    .partitionId("7")
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            filesToRemoveFileInPartitionEntries.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        FileInfo newLeftFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-left-new")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .numberOfRecords(600L)
                .lastStateStoreUpdateTime(10_000_000L)
                .build();
        FileInfo newRightFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-right-new")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("7")
                .numberOfRecords(400L)
                .lastStateStoreUpdateTime(10_000_000L)
                .build();

        // When
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(filesToRemoveFileInPartitionEntries, newLeftFileInfo, newRightFileInfo);

        // Then
        assertThat(dynamoDBStateStore.getFileInPartitionList()).containsExactlyInAnyOrder(newLeftFileInfo, newRightFileInfo);
        assertThat(dynamoDBStateStore.getFileLifecycleList()).containsExactlyInAnyOrder(
                filesToRemoveFileInPartitionEntries.get(0).cloneWithStatus(FileStatus.ACTIVE),
                filesToRemoveFileInPartitionEntries.get(1).cloneWithStatus(FileStatus.ACTIVE),
                filesToRemoveFileInPartitionEntries.get(2).cloneWithStatus(FileStatus.ACTIVE),
                filesToRemoveFileInPartitionEntries.get(3).cloneWithStatus(FileStatus.ACTIVE),
                newLeftFileInfo.cloneWithStatus(FileStatus.ACTIVE),
                newRightFileInfo.cloneWithStatus(FileStatus.ACTIVE)
        );
    }

    @Test
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFileShouldFailIfFileInPartitionRecordDoesntExist() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileStatus.ACTIVE)
                    .partitionId("7")
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .build();
            files.add(fileInfo);
        }
        dynamoDBStateStore.addFiles(files);
        FileInfo newFileInfo = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file-new")
                .fileStatus(FileStatus.ACTIVE)
                .partitionId("7")
                .numberOfRecords(5000L)
                .build();
        //  - One of the file-in-partition entries is removed
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(files.get(3)), newFileInfo);

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(files, newFileInfo))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldAtomicallyUpdateJobStatusOfFiles() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileStatus.ACTIVE)
                    .partitionId("8")
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When
        dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files);

        // Then
        assertThat(dynamoDBStateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("jobId")
                .containsExactlyInAnyOrderElementsOf(files)
                .extracting(FileInfo::getJobId)
                .containsOnly(jobId);
        assertThat(dynamoDBStateStore.getFileLifecycleList())
                .containsExactlyInAnyOrder(files.get(0), files.get(1), files.get(2), files.get(3));
        assertThat(dynamoDBStateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldNotAtomicallyUpdateJobStatusOfFilesIfFileInfoNotPresent() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileStatus.ACTIVE)
                    .partitionId("8")
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            files.add(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When / Then
        assertThatThrownBy(() -> dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldNotAtomicallyCreateJobAndUpdateJobStatusOfFilesWhenJobIdAlreadySet() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileStatus.ACTIVE)
                    .partitionId("9")
                    .numberOfRecords(1000L)
                    .jobId("compactionJob")
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(i * 1_000_000L)
                    .build();
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }
        String jobId = UUID.randomUUID().toString();

        // When / Then
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdateJobStatusOfFiles(jobId, files))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldDeleteFileLifecycleEntries() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file1")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("4")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file2")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("5")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(2_000_000L)
                .build();
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(fileInfo1),
                fileInfo2);

        // When
        dynamoDBStateStore.deleteFileLifecycleEntries(Collections.singletonList(fileInfo1.getFilename()));

        // Then
        assertThat(dynamoDBStateStore.getFileInPartitionList()).containsExactly(fileInfo2);
        assertThat(dynamoDBStateStore.getFileLifecycleList()).containsExactly(fileInfo2.cloneWithStatus(FileStatus.ACTIVE));
        assertThat(dynamoDBStateStore.getReadyForGCFiles()).isExhausted();
    }

    @Test
    public void shouldReturnCorrectFileInPartitionList() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10_000; i++) { // 10,000 figure chosen to ensure results returned from Dynamo are paged
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileStatus.FILE_IN_PARTITION)
                    .partitionId("" + i)
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .build();
            dynamoDBStateStore.addFile(fileInfo);
            expected.add(fileInfo);
        }

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getFileInPartitionList();

        // Then
        assertThat(fileInfos).hasSize(10000).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldReturnCorrectFileLifecycleList() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> fileInfos = new ArrayList<>();
        // - Create 10,000 FileInfos
        for (int i = 0; i < 10_000; i++) { // 10,000 figure chosen to ensure results returned from Dynamo are paged
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileStatus.FILE_IN_PARTITION)
                    .partitionId("" + i)
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .build();
            fileInfos.add(fileInfo);
        }
        Map<String, FileInfo> expected = new HashMap<>();
        for (int i = 0; i < 9_999; i++) {
            dynamoDBStateStore.addFile(fileInfos.get(i));
            expected.put(fileInfos.get(i).getFilename(), fileInfos.get(i).cloneWithStatus(FileStatus.ACTIVE));
        }
        // - Remove the file-in-partition record for the 0th file and add file 9999
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(fileInfos.get(0)), fileInfos.get(9_999));
        // - The 0th file can have its status updated to GARBAGE_COLLECTION_PENDING
        dynamoDBStateStore.findFilesThatShouldHaveStatusOfGCPending();
        expected.put("file-0", expected.get("file-0").cloneWithStatus(FileStatus.GARBAGE_COLLECTION_PENDING));
        expected.put("file-9999", fileInfos.get(9999).cloneWithStatus(FileStatus.ACTIVE));

        // When
        List<FileInfo> fileLifecycleList = dynamoDBStateStore.getFileLifecycleList();

        // Then
        assertThat(fileLifecycleList).hasSize(10_000);
        List<FileInfo> expectedWithConstantLastStateStoreUpdateTime = expected.values()
                .stream()
                .map(f -> f.toBuilder().lastStateStoreUpdateTime(Instant.ofEpochSecond(100L)).build())
                .collect(Collectors.toList());
        List<FileInfo> actualWithConstantLastStateStoreUpdateTime = fileLifecycleList
                .stream()
                .map(f -> f.toBuilder().lastStateStoreUpdateTime(Instant.ofEpochSecond(100L)).build())
                .collect(Collectors.toList());
        assertThat(actualWithConstantLastStateStoreUpdateTime.size()).isEqualTo(expectedWithConstantLastStateStoreUpdateTime.size());
        Map<String, FileInfo> actualMap = new HashMap<>();
        for (FileInfo f : actualWithConstantLastStateStoreUpdateTime) {
            actualMap.put(f.getFilename(), f);
        }
        Map<String, FileInfo> expectedMap = new HashMap<>();
        for (FileInfo f : expectedWithConstantLastStateStoreUpdateTime) {
            expectedMap.put(f.getFilename(), f);
        }
        assertThat(actualMap).isEqualTo(expectedMap);
        // NB Using containsExactlyInAnyOrder for the above asserts results in the test not terminating
    }

    @Test
    public void shouldReturnCorrectActiveFileList() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> fileInfos = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) { // 10,000 figure chosen to ensure results returned from Dynamo are paged
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileStatus.FILE_IN_PARTITION)
                    .partitionId("" + i)
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create(1L))
                    .maxRowKey(Key.create(10L))
                    .lastStateStoreUpdateTime(1_000_000L)
                    .build();
            fileInfos.add(fileInfo);
        }
        Map<String, FileInfo> expected = new HashMap<>();
        for (int i = 0; i < 9_999; i++) {
            dynamoDBStateStore.addFile(fileInfos.get(i));
            expected.put(fileInfos.get(i).getFilename(), fileInfos.get(i).cloneWithStatus(FileStatus.ACTIVE));
        }
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(fileInfos.get(0)), fileInfos.get(9_999));
        dynamoDBStateStore.findFilesThatShouldHaveStatusOfGCPending();
        expected.remove("file-0");
        expected.put("file-9999", fileInfos.get(9_999).cloneWithStatus(FileStatus.ACTIVE));

        // When
        List<FileInfo> activeFileList = dynamoDBStateStore.getActiveFileList();

        // Then
        assertThat(activeFileList).hasSize(9_999).containsExactlyInAnyOrderElementsOf(expected.values());
    }

    @Test
    public void shouldFindFilesThatShouldHaveStatusOfGCPending() throws Exception {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new StringType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoFactory factory = FileInfoFactory.builder().schema(schema).partitionTree(tree).lastStateStoreUpdate(Instant.now()).build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "a", "b");
        FileInfo file3 = factory.rootFile("file3", 100L, "a", "b");
        dynamoDBStateStore.addFile(file1);
        dynamoDBStateStore.addFile(file2);
        dynamoDBStateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file1), file3);

        // When 1
        dynamoDBStateStore.findFilesThatShouldHaveStatusOfGCPending();

        // Then 1
        // - Check that file1 has status of GARBAGE_COLLECTION_PENDING
        FileInfo fileInfoForFile1 = dynamoDBStateStore.getFileLifecycleList().stream()
                .filter(fi -> fi.getFilename().equals(file1.getFilename()))
                .findFirst()
                .get();
        assertThat(fileInfoForFile1.getFileStatus()).isEqualTo(GARBAGE_COLLECTION_PENDING);
        // - Check that file2 and file3 have statuses of ACTIVE
        List<FileInfo> fileInfoForFile2 = dynamoDBStateStore.getFileLifecycleList().stream()
                .filter(fi -> fi.getFilename().equals(file2.getFilename()) || fi.getFilename().equals(file3.getFilename()))
                .collect(Collectors.toList());
        assertThat(fileInfoForFile2)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(file2.cloneWithStatus(ACTIVE), file3.cloneWithStatus(ACTIVE));

        // When 2
        // - Run findFilesThatShouldHaveStatusOfGCPending again - nothing should change (and the state store should not
        // update the update time).
        dynamoDBStateStore.findFilesThatShouldHaveStatusOfGCPending();
        FileInfo fileInfoForFile1SecondTime = dynamoDBStateStore.getFileLifecycleList().stream()
                .filter(fi -> fi.getFilename().equals(file1.getFilename()))
                .findFirst()
                .get();
        assertThat(fileInfoForFile1SecondTime.getFileStatus()).isEqualTo(GARBAGE_COLLECTION_PENDING);
        assertThat(fileInfoForFile1SecondTime.getLastStateStoreUpdateTime()).isEqualTo(fileInfoForFile1.getLastStateStoreUpdateTime());
        // - File2 and file3 should still have statuses of ACTIVE
        List<FileInfo> fileInfoForFile2SecondTime = dynamoDBStateStore.getFileLifecycleList().stream()
                .filter(fi -> fi.getFilename().equals(file2.getFilename()) || fi.getFilename().equals(file3.getFilename()))
                .collect(Collectors.toList());
        assertThat(fileInfoForFile2SecondTime)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(file2.cloneWithStatus(ACTIVE), file3.cloneWithStatus(ACTIVE));
    }

    @Test
    public void shouldReturnCorrectReadyForGCFilesIterator() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(Collections.singletonList("root"), Collections.emptyList())
                .buildTree();
        FileInfoStore store = new InMemoryFileInfoStore(4);
        //  - A file which should be garbage collected immediately
        //     (NB Need to add file, which adds file-in-partition and lifecycle enrties, then simulate a compaction
        //      to remove the file in partition entries, then set the status to ready for GC)
        FileInfoFactory factory = FileInfoFactory.builder()
                .schema(schema)
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(System.currentTimeMillis() - 8000))
                .build();
        FileInfo file1 = factory.rootFile("file1", 100L, "a", "b");
        FileInfo file2 = factory.rootFile("file2", 100L, "a", "b");
        store.addFile(file1);
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file1.cloneWithStatus(FileStatus.FILE_IN_PARTITION)),
                file2);
        store.findFilesThatShouldHaveStatusOfGCPending();
        //  - An active file which should not be garbage collected immediately
        FileInfoFactory factory2 = FileInfoFactory.builder()
                .schema(schema)
                .partitionTree(tree)
                .lastStateStoreUpdate(Instant.ofEpochMilli(System.currentTimeMillis() + 4000L))
                .build();
        FileInfo file3 = factory2.rootFile("file3", 100L, "a", "b");
        store.addFile(file3);
        FileInfo file4 = factory2.rootFile("file4", 100L, "a", "b");
        store.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(Collections.singletonList(file3.cloneWithStatus(FileStatus.FILE_IN_PARTITION)),
                file4);
        //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
        //      just been marked as ready for GC
        FileInfo file5 = factory2.rootFile("file5", 100L, "a", "b");
        store.addFile(file5);

        // When / Then 1
        Thread.sleep(5000L);
        List<String> readyForGCFiles = new ArrayList<>();
        store.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        assertThat(readyForGCFiles).hasSize(1);
        assertThat(readyForGCFiles.get(0)).isEqualTo("file1");

        // When / Then 2
        store.findFilesThatShouldHaveStatusOfGCPending();
        Thread.sleep(5000L);
        readyForGCFiles.clear();
        store.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        assertThat(readyForGCFiles).hasSize(2);
        assertThat(readyForGCFiles.stream().collect(Collectors.toSet())).containsExactlyInAnyOrder("file1", "file3");
    }

    @Test
    public void shouldReturnFileInPartitionInfosWithNoJobId() throws StateStoreException {
        // Given
        Schema schema = schemaWithKeyAndValueWithTypes(new LongType(), new StringType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file1")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("1")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .lastStateStoreUpdateTime(1_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo1);
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file2")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("2")
                .numberOfRecords(1000L)
                .minRowKey(Key.create(20L))
                .maxRowKey(Key.create(29L))
                .lastStateStoreUpdateTime(2_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo2);
        FileInfo fileInfo3 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("file3")
                .fileStatus(FileStatus.FILE_IN_PARTITION)
                .partitionId("3")
                .numberOfRecords(1000L)
                .jobId("job1")
                .minRowKey(Key.create(100L))
                .maxRowKey(Key.create(10000L))
                .lastStateStoreUpdateTime(3_000_000L)
                .build();
        dynamoDBStateStore.addFile(fileInfo3);

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getFileInPartitionInfosWithNoJobId();

        // Then
        assertThat(fileInfos).containsExactlyInAnyOrder(fileInfo1, fileInfo2);
    }

    @Test
    public void shouldReturnOnlyActiveFilesWithNoJobIdWhenPaging() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        Set<FileInfo> expected = new HashSet<>();
        for (int i = 0; i < 10_000; i++) { // 10,000 figure chosen to ensure results returned from Dyanmo are paged
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file-" + i)
                    .fileStatus(FileStatus.FILE_IN_PARTITION)
                    .partitionId("" + i)
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create((long) 2 * i))
                    .maxRowKey(Key.create((long) 2 * i + 1))
                    .lastStateStoreUpdateTime((long) i * 1_000)
                    .build();
            dynamoDBStateStore.addFile(fileInfo);
            expected.add(fileInfo);
        }

        // When
        List<FileInfo> fileInfos = dynamoDBStateStore.getFileInPartitionInfosWithNoJobId();

        // Then
        assertThat(fileInfos).hasSize(10000).containsExactlyInAnyOrderElementsOf(expected);
    }

    // TODO shouldCorrectlyStorePartitionWithMultidimensionalKeyType

    @Test
    public void shouldReturnCorrectPartitionToFileInPartitionMap() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(new LongType())
                    .filename("file" + i)
                    .fileStatus(FileStatus.ACTIVE)
                    .partitionId("" + (i % 5))
                    .numberOfRecords(1000L)
                    .minRowKey(Key.create((long) i % 5))
                    .maxRowKey(Key.create((long) i % 5))
                    .build();
            files.add(fileInfo);
            dynamoDBStateStore.addFile(fileInfo);
        }

        // When
        Map<String, List<String>> partitionToFileMapping = dynamoDBStateStore.getPartitionToFileInPartitionMap();

        // Then
        assertThat(partitionToFileMapping.entrySet()).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(partitionToFileMapping.get("" + i)).hasSize(2);
            Set<String> expected = new HashSet<>();
            expected.add(files.get(i).getFilename());
            expected.add(files.get(i + 5).getFilename());
            assertThat(new HashSet<>(partitionToFileMapping.get("" + i))).isEqualTo(expected);
        }
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithLongKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new LongType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList(100L))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithStringKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new StringType());
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Collections.singletonList("B"))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithByteArrayKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
        byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyInitialisePartitionsWithMultidimensionalKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithTwoRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        byte[] splitPoint1 = new byte[]{1, 2, 3, 4};
        byte[] splitPoint2 = new byte[]{5, 6, 7, 8, 9};
        List<Partition> partitions = new PartitionsFromSplitPoints(schema, Arrays.asList(splitPoint1, splitPoint2))
                .construct();
        StateStore stateStore = getStateStore(schema, partitions);

        // When / Then
        assertThat(stateStore.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions);
    }

    @Test
    public void shouldCorrectlyStoreNonLeafPartitionWithByteArrayKeyType() throws StateStoreException {
        // Given
        Schema schema = schemaWithSingleRowKeyType(new ByteArrayType());
        byte[] min = new byte[]{1, 2, 3, 4};
        byte[] max = new byte[]{5, 6, 7, 8, 9};
        Range range = new RangeFactory(schema).createRange("key", min, max);
        Partition partition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(new Region(range))
                .id("id")
                .leafPartition(false)
                .parentPartitionId("P")
                .childPartitionIds(new ArrayList<>())
                .dimension(0)
                .build();
        StateStore dynamoDBStateStore = getStateStore(schema, Collections.singletonList(partition));

        // When
        Partition retrievedPartition = dynamoDBStateStore.getAllPartitions().get(0);

        // Then
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMin()).containsExactly((byte[]) partition.getRegion().getRange("key").getMin());
        assertThat((byte[]) retrievedPartition.getRegion().getRange("key").getMax()).containsExactly((byte[]) partition.getRegion().getRange("key").getMax());
        assertThat(retrievedPartition.getId()).isEqualTo(partition.getId());
        assertThat(retrievedPartition.getParentPartitionId()).isEqualTo(partition.getParentPartitionId());
        assertThat(retrievedPartition.getChildPartitionIds()).isEqualTo(partition.getChildPartitionIds());
        assertThat(retrievedPartition.getDimension()).isEqualTo(partition.getDimension());
    }

    @Test
    public void shouldReturnAllPartitions() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region0 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, 1L));
        Partition partition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region0)
                .id("id0")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region1 = new Region(rangeFactory.createRange(field, 1L, 100L));
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region1)
                .id("id1")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, 100L, 200L));
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region2)
                .id("id2")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region3 = new Region(rangeFactory.createRange(field, 200L, null));
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region3)
                .id("id3")
                .leafPartition(true)
                .parentPartitionId("root")
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        StateStore dynamoDBStateStore = getStateStore(schema, Arrays.asList(partition0, partition1, partition2, partition3));

        // When
        List<Partition> retrievedPartitions = dynamoDBStateStore.getAllPartitions();
        retrievedPartitions.sort((p1, p2) -> {
            long p1Key = (long) p1.getRegion().getRange("key").getMin();
            long p2Key = (long) p2.getRegion().getRange("key").getMin();
            if (p1Key < p2Key) {
                return -1;
            } else if (p1Key == p2Key) {
                return 0;
            }
            return 1;
        });

        // Then
        assertThat(retrievedPartitions).containsExactly(partition0, partition1, partition2, partition3);
    }

    @Test
    public void shouldReturnLeafPartitions() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition rootPartition = dynamoDBStateStore.getAllPartitions().get(0);
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, 1L));
        Partition partition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region1)
                .id("id1")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, 1L, null));
        Partition partition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region2)
                .id("id2")
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        rootPartition = rootPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()))
                .build();
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, partition1, partition2);
        Region region3 = new Region(rangeFactory.createRange(field, 1L, 9L));
        Partition partition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region3)
                .id("id3")
                .leafPartition(true)
                .parentPartitionId(partition2.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        Region region4 = new Region(rangeFactory.createRange(field, 9L, null));
        Partition partition4 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(region4)
                .id("id4")
                .leafPartition(true)
                .parentPartitionId(partition2.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        partition2 = partition2.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList(partition3.getId(), partition4.getId()))
                .build();
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(partition2, partition3, partition4);

        // When
        List<Partition> retrievedPartitions = dynamoDBStateStore.getLeafPartitions();
        retrievedPartitions.sort((p1, p2) -> {
            long p1Key = (long) p1.getRegion().getRange("key").getMin();
            long p2Key = (long) p2.getRegion().getRange("key").getMin();
            if (p1Key < p2Key) {
                return -1;
            } else if (p1Key == p2Key) {
                return 0;
            }
            return 1;
        });

        // Then
        assertThat(retrievedPartitions).containsExactly(partition1, partition3, partition4);
    }

    @Test
    public void shouldUpdatePartitions() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);

        // When
        parentPartition = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        parentPartition = parentPartition.toBuilder().dimension(1).build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .dimension(-1)
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, 0L, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .dimension(-1)
                .build();
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);

        // Then
        assertThat(dynamoDBStateStore.getAllPartitions())
                .containsExactlyInAnyOrder(parentPartition, childPartition1, childPartition2);
    }

    @Test
    public void shouldNotUpdatePartitionsIfLeafStatusChanges() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, 0L, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId(parentPartition.getId())
                .build();
        dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, childPartition1, childPartition2);

        // When / Then
        //  - Attempting to split something that has already been split should fail
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentIsLeaf() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder().childPartitionIds(Arrays.asList("child1", "child2")).build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereChildrenWrong() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child3", "child2")) // Wrong children
                .build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereParentWrong() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("notparent") // Wrong parent
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldThrowExceptionWithPartitionSplitRequestWhereNewPartitionIsNotLeaf() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);
        Partition parentPartition = dynamoDBStateStore.getAllPartitions().get(0);
        parentPartition = parentPartition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList("child1", "child2"))
                .build();
        Region region1 = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, 0L));
        Partition childPartition1 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(true)
                .id("child1")
                .region(region1)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();
        Region region2 = new Region(rangeFactory.createRange(field, 0L, Long.MAX_VALUE));
        Partition childPartition2 = Partition.builder()
                .rowKeyTypes(new LongType())
                .leafPartition(false) // Not leaf
                .id("child2")
                .region(region2)
                .childPartitionIds(new ArrayList<>())
                .parentPartitionId("parent")
                .build();

        // When / Then
        Partition finalParentPartition = parentPartition;
        assertThatThrownBy(() ->
                dynamoDBStateStore.atomicallyUpdatePartitionAndCreateNewOnes(finalParentPartition, childPartition1, childPartition2))
                .isInstanceOf(StateStoreException.class);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForIntKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(rangeFactory.createRange(field, Integer.MIN_VALUE, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForLongKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(rangeFactory.createRange(field, Long.MIN_VALUE, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(Collections.singletonList(new LongType()))
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForStringKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(rangeFactory.createRange(field, "", null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(Collections.singletonList(new StringType()))
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseRootPartitionCorrectlyForByteArrayKey() throws StateStoreException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        StateStore dynamoDBStateStore = getStateStore(schema);

        // When
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();

        // Then
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(rangeFactory.createRange(field, new byte[]{}, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(Collections.singletonList(new ByteArrayType()))
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }
}
