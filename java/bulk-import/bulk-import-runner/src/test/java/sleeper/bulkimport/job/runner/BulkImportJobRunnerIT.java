/*
 * Copyright 2022 Crown Copyright
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
package sleeper.bulkimport.job.runner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.runner.dataframe.BulkImportJobDataframeRunner;
import sleeper.bulkimport.job.runner.rdd.BulkImportJobRDDRunner;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

@RunWith(Parameterized.class)
public class BulkImportJobRunnerIT {

    @Parameters
    public static Collection<Object[]> getParameters() {
        return Lists.newArrayList(new Object[][]{{new BulkImportJobDataframeRunner()}, {new BulkImportJobRDDRunner()}});
    }

    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3
    );

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private final BulkImportJobRunner runner;

    public BulkImportJobRunnerIT(BulkImportJobRunner runner) {
        this.runner = runner;
    }

    private AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }

    private static AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .build();
    }

    public InstanceProperties createInstanceProperties(AmazonS3 s3Client, String dir) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, dir);
        instanceProperties.set(JARS_BUCKET, "");
        instanceProperties.set(ACCOUNT, "");
        instanceProperties.set(REGION, "");
        instanceProperties.set(VERSION, "");
        instanceProperties.set(VPC_ID, "");
        instanceProperties.set(SUBNET, "");
        instanceProperties.set(TABLE_PROPERTIES, "");
        instanceProperties.set(BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE, "0");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));

        return instanceProperties;
    }

    @BeforeClass
    public static void setSparkProperties() {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.app.name", "bulk import");
    }

    @AfterClass
    public static void clearSparkProperties() {
        System.clearProperty("spark.master");
        System.clearProperty("spark.app.name");
    }

    public TableProperties createTable(AmazonS3 s3,
                                       AmazonDynamoDB dynamoDB,
                                       InstanceProperties instanceProperties,
                                       String tableName,
                                       String dataBucket,
                                       Schema schema) throws IOException, StateStoreException {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, dataBucket);
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, tableName + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, tableName + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "10");
        s3.createBucket(tableProperties.get(DATA_BUCKET));
        tableProperties.saveToS3(s3);

        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(instanceProperties,
                tableProperties, dynamoDB);
        dynamoDBStateStoreCreator.create();
        return tableProperties;
    }

    private Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(
                        new Field("value1", new StringType()),
                        new Field("value2", new ListType(new IntType())),
                        new Field("value3", new MapType(new StringType(), new LongType())))
                .build();
    }

    @Test
    public void shouldImportDataSinglePartition() throws IOException, StateStoreException {
        // Given
        //  - AWS Clients
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        //  - Schema
        Schema schema = getSchema();
        //  - Instance and table properties
        String dataDir = folder.newFolder().getAbsolutePath();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        //  - Set min number of partitions for a coalesce to 100 so that no coalesce will happen
        instanceProperties.set(BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE, "100");
        String tableName = UUID.randomUUID().toString();
        String localDir = UUID.randomUUID().toString();
        TableProperties tableProperties = createTable(s3Client, dynamoDBClient, instanceProperties, tableName, localDir, schema);
        //  - Write some data to be imported
        List<Record> records = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
        }
        Collections.shuffle(records);
        ParquetRecordWriter writer = new ParquetRecordWriter(new Path(dataDir + "/import/a.parquet"),
                SchemaConverter.getSchema(getSchema()), getSchema(), CompressionCodecName.SNAPPY, 10000, 10000);
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add("/import/a.parquet");
        //  - State store
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();

        // When
        runner.init(instanceProperties, s3Client, dynamoDBClient);
        runner.run(new BulkImportJob.Builder().id("my-job").files(inputFiles).tableName(tableName).build());

        // Then
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        List<Record> readRecords = new ArrayList<>();
        for (FileInfo fileInfo : activeFiles) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileInfo.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                List<Record> sortedRecordsInThisFile = new ArrayList<>(recordsInThisFile);
                sortRecords(sortedRecordsInThisFile);
                assertThat(recordsInThisFile).isEqualTo(sortedRecordsInThisFile);
            }
        }
        assertThat(readRecords).hasSameSizeAs(records);

        List<Record> expectedRecords = new ArrayList<>(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
    }

    @Test
    public void shouldNotCoalesceIfSmallNumberOfLeafPartitions() throws IOException, StateStoreException {
        // Given
        //  - AWS Clients
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        //  - Schema
        Schema schema = getSchema();
        //  - Instance and table properties
        String dataDir = folder.newFolder().getAbsolutePath();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        //  - Set min number of partitions for a coalesce to 100 so that no coalesce will happen
        instanceProperties.set(BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE, "100");
        String tableName = UUID.randomUUID().toString();
        String localDir = UUID.randomUUID().toString();
        TableProperties tableProperties = createTable(s3Client, dynamoDBClient, instanceProperties, tableName, localDir, schema);
        //  - Write some data to be imported
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
        }
        Collections.shuffle(records);
        ParquetRecordWriter writer = new ParquetRecordWriter(new Path(dataDir + "/import/a.parquet"),
                SchemaConverter.getSchema(getSchema()), getSchema(), CompressionCodecName.SNAPPY, 10000, 10000);
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
        // Shuffle again so that the data is sorted differently in the two files
        Collections.shuffle(records);
        writer = new ParquetRecordWriter(new Path(dataDir + "/import/b.parquet"),
                SchemaConverter.getSchema(getSchema()), getSchema(), CompressionCodecName.SNAPPY, 10000, 10000);
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add("/import/a.parquet");
        inputFiles.add("/import/b.parquet");
        //  - State store
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();

        // When
        runner.init(instanceProperties, s3Client, dynamoDBClient);
        runner.run(new BulkImportJob.Builder().id("my-job").files(inputFiles).tableName(tableName).build());

        // Then
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        List<Record> readRecords = new ArrayList<>();
        for (FileInfo fileInfo : activeFiles) {
            try (ParquetRecordReader reader = new ParquetRecordReader(new Path(fileInfo.getFilename()), schema)) {
                List<Record> recordsInThisFile = new ArrayList<>();
                Record record = reader.read();
                while (null != record) {
                    Record clonedRecord = new Record(record);
                    readRecords.add(clonedRecord);
                    recordsInThisFile.add(clonedRecord);
                    record = reader.read();
                }
                List<Record> sortedRecordsInThisFile = new ArrayList<>(recordsInThisFile);
                sortRecords(sortedRecordsInThisFile);
                assertThat(recordsInThisFile).isEqualTo(sortedRecordsInThisFile);
            }
        }
        assertThat(readRecords).hasSize(2 * records.size());

        List<Record> expectedRecords = new ArrayList<>();
        expectedRecords.addAll(records);
        expectedRecords.addAll(records);
        sortRecords(expectedRecords);
        sortRecords(readRecords);
        assertThat(readRecords).isEqualTo(expectedRecords);
    }

    @Test
    public void shouldImportDataMultiplePartitions() throws IOException, StateStoreException {
        // Given
        //  - AWS Clients
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        //  - Schema
        Schema schema = getSchema();
        //  - Instance and table properties
        String dataDir = folder.newFolder().getAbsolutePath();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        String tableName = UUID.randomUUID().toString();
        String localDir = UUID.randomUUID().toString();
        TableProperties tableProperties = createTable(s3Client, dynamoDBClient, instanceProperties, tableName, localDir, schema);
        //  - Write some data to be imported
        List<Record> records = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
        }
        Collections.shuffle(records);
        ParquetRecordWriter writer = new ParquetRecordWriter(new Path(dataDir + "/import/a.parquet"),
                SchemaConverter.getSchema(getSchema()), getSchema(), CompressionCodecName.SNAPPY, 10000, 10000);
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add("/import/a.parquet");
        //  - State store
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise(new PartitionsFromSplitPoints(schema, Collections.singletonList(50)).construct());

        // When
        runner.init(instanceProperties, s3Client, dynamoDBClient);
        runner.run(new BulkImportJob.Builder().id("my-job").files(inputFiles).tableName(tableName).build());

        // Then
        List<Record> leftPartition = records.stream()
                .filter(record -> ((int) record.get("key")) < 50)
                .sorted((record1, record2) -> {
                    int key1 = (int) record1.get("key");
                    int key2 = (int) record2.get("key");
                    return key1 - key2;
                })
                .collect(Collectors.toList());
        List<Record> rightPartition = records.stream()
                .filter(record -> ((int) record.get("key")) >= 50)
                .sorted((record1, record2) -> {
                    int key1 = (int) record1.get("key");
                    int key2 = (int) record2.get("key");
                    return key1 - key2;
                })
                .collect(Collectors.toList());
        assertThat(stateStore.getActiveFiles())
                .extracting(FileInfo::getNumberOfRecords,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactlyInAnyOrder(
                        tuple(50L, leftPartition),
                        tuple(50L, rightPartition));
    }

    @Test
    public void shouldImportLargeAmountOfDataMultiplePartitions() throws IOException, StateStoreException {
        // Given
        //  - AWS Clients
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        //  - Schema
        Schema schema = getSchema();
        //  - Instance and table properties
        String dataDir = folder.newFolder().getAbsolutePath();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        String tableName = UUID.randomUUID().toString();
        String localDir = UUID.randomUUID().toString();
        TableProperties tableProperties = createTable(s3Client, dynamoDBClient, instanceProperties, tableName, localDir, schema);
        //  - Write some data to be imported
        List<Record> records = new ArrayList<>(100000);
        List<Object> splitPoints = new ArrayList<>();
        for (int i = 0; i < 100000; i++) {
            if (i % 1000 == 0) {
                splitPoints.add(i);
            }
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);

        }
        Collections.shuffle(records);
        ParquetRecordWriter writer = new ParquetRecordWriter(new Path(dataDir + "/import/a.parquet"),
                SchemaConverter.getSchema(getSchema()), getSchema(), CompressionCodecName.SNAPPY, 10000, 10000);
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
        List<String> inputFiles = new ArrayList<>();
        inputFiles.add("/import/a.parquet");
        //  - State store
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise(new PartitionsFromSplitPoints(schema, splitPoints).construct());

        // When
        runner.init(instanceProperties, s3Client, dynamoDBClient);
        runner.run(new BulkImportJob.Builder().id("my-job").files(inputFiles).tableName(tableName).build());

        // Then
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        for (Partition leaf : leafPartitions) {
            Integer minRowKey = (Integer) leaf.getRegion().getRange(schema.getRowKeyFieldNames().get(0)).getMin();
            if (Integer.MIN_VALUE == minRowKey) {
                continue;
            }
            List<FileInfo> relevantFiles = activeFiles.stream()
                    .filter(af -> af.getPartitionId().equals(leaf.getId()))
                    .collect(Collectors.toList());

            long totalRecords = relevantFiles.stream()
                    .map(FileInfo::getNumberOfRecords)
                    .reduce(Long::sum)
                    .get();

            assertThat(totalRecords).isEqualTo(1000L);

            relevantFiles.stream()
                    .map(af -> {
                        try {
                            return new ParquetRecordReader(new Path(af.getFilename()), schema);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .map(reader -> {
                        List<Record> recordsRead = new ArrayList<>();
                        Record record;
                        try {
                            record = reader.read();
                            while (record != null) {
                                recordsRead.add(record);
                                record = reader.read();
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return recordsRead;
                    })
                    .forEach(read -> {
                        List<Record> sorted = read.stream()
                                .sorted(new RecordComparator(schema))
                                .collect(Collectors.toList());

                        assertThat(read).isEqualTo(sorted);
                    });
        }
    }

    @Test
    public void shouldNotThrowExceptionIfProvidedWithDirectoryWhichContainsParquetAndNonParquetFiles() throws IOException, StateStoreException {
        // Given
        //  - AWS Clients
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        //  - Schema
        Schema schema = getSchema();
        //  - Instance and table properties
        String dataDir = folder.newFolder().getAbsolutePath();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client, dataDir);
        String tableName = UUID.randomUUID().toString();
        String localDir = UUID.randomUUID().toString();
        TableProperties tableProperties = createTable(s3Client, dynamoDBClient, instanceProperties, tableName, localDir, schema);
        //  - Write some data to be imported
        List<Record> records = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("sort", (long) i);
            record.put("value1", "" + i);
            record.put("value2", Arrays.asList(1, 2, 3));
            Map<String, Long> map = new HashMap<>();
            map.put("A", 1L);
            record.put("value3", map);
            records.add(record);
        }
        Collections.shuffle(records);
        ParquetRecordWriter writer = new ParquetRecordWriter(new Path(dataDir + "/import/a.parquet"),
                SchemaConverter.getSchema(getSchema()), getSchema(), CompressionCodecName.SNAPPY, 10000, 10000);
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dataDir + "/import/b.txt"))) {
            bufferedWriter.append("test");
        }

        //  - State store
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();

        // When
        runner.init(instanceProperties, s3Client, dynamoDBClient);
        runner.run(new BulkImportJob.Builder().id("my-job").files(Lists.newArrayList("/import/")).tableName(tableName).build());

        // Then
        String expectedPartitionId = stateStore.getAllPartitions().get(0).getId();
        sortRecords(records);
        assertThat(stateStore.getActiveFiles())
                .extracting(FileInfo::getNumberOfRecords, FileInfo::getPartitionId,
                        file -> readRecords(file.getFilename(), schema))
                .containsExactly(tuple(100L, expectedPartitionId, records));
    }

    private List<Record> readRecords(String filename, Schema schema) {
        try (ParquetRecordReader reader = new ParquetRecordReader(new Path(filename), schema)) {
            List<Record> readRecords = new ArrayList<>();
            Record record = reader.read();
            while (null != record) {
                readRecords.add(new Record(record));
                record = reader.read();
            }
            return readRecords;
        } catch (IOException e) {
            throw new RuntimeException("Failed reading records", e);
        }
    }

    private void sortRecords(List<Record> records) {
        records.sort((record1, record2) -> {
            int key1 = (int) record1.get("key");
            int key2 = (int) record2.get("key");
            return key1 - key2;
        });
    }
}
