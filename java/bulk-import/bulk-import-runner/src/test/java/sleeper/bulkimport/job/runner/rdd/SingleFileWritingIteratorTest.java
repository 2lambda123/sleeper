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
package sleeper.bulkimport.job.runner.rdd;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SingleFileWritingIteratorTest {

    @TempDir
    public java.nio.file.Path tempFolder;

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final Schema schema = createSchema();
    private TableProperties tableProperties;

    @BeforeEach
    void setUp() {
        tableProperties = createTableProperties(instanceProperties, schema, tempFolder);
    }

    @Nested
    @DisplayName("Output a single file")
    class OutputSingleFile {

        private final Iterator<Row> input = Lists.newArrayList(
                RowFactory.create("a", 1, 2),
                RowFactory.create("b", 1, 2),
                RowFactory.create("c", 1, 2),
                RowFactory.create("d", 1, 2)
        ).iterator();

        @Test
        void shouldWriteAllRecordsToAParquetFile() {
            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRecords(input);

            // Then
            assertThat(fileWritingIterator).toIterable()
                    .extracting(row -> readRecords(readPathFromOutputFileMetadata(row)))
                    .containsExactly(
                            Arrays.asList(
                                    createRecord("a", 1, 2),
                                    createRecord("b", 1, 2),
                                    createRecord("c", 1, 2),
                                    createRecord("d", 1, 2)));
        }

        @Test
        void shouldOutputMetadataPointingToSingleFileInFolderForPartition() {
            // Given
            PartitionTree partitionTree = createPartitionsBuilder()
                    .singlePartition("test-partition")
                    .buildTree();

            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRecordsWithPartitionsAndOutputFilename(
                    input, partitionTree, "test-file");

            // Then
            assertThat(fileWritingIterator).toIterable()
                    .containsExactly(RowFactory.create(
                            "test-partition",
                            "file://" + tempFolder + "/partition_test-partition/test-file.parquet",
                            4));
        }
    }

    @Nested
    @DisplayName("Infer output partition")
    class InferOutputPartition {
        private SingleFileWritingIterator fileWritingIterator;

        @BeforeEach
        void setUp() {
            // Given
            Iterator<Row> input = Lists.newArrayList(
                    RowFactory.create("a", 1, 2),
                    RowFactory.create("b", 1, 2),
                    RowFactory.create("d", 1, 2),
                    RowFactory.create("e", 1, 2)
            ).iterator();
            PartitionTree partitionTree = createPartitionsBuilder()
                    .leavesWithSplits(List.of("left", "right"), List.of("c"))
                    .parentJoining("root", "left", "right")
                    .buildTree();

            // When
            fileWritingIterator = createIteratorOverRecordsWithPartitions(input, partitionTree);
        }

        @Test
        void shouldInferPartitionIdFromFirstRecordWhenSomeRecordsAreInDifferentPartitions() {
            // Then
            assertThat(fileWritingIterator).toIterable()
                    .extracting(SingleFileWritingIteratorTest.this::readPartitionIdFromOutputFileMetadata)
                    .containsExactly("left");
        }

        @Test
        void shouldAssumeAllRowsAreInTheSamePartition() {
            // Then
            assertThat(fileWritingIterator).toIterable()
                    .extracting(row -> readRecords(readPathFromOutputFileMetadata(row)))
                    .containsExactly(
                            Arrays.asList(
                                    createRecord("a", 1, 2),
                                    createRecord("b", 1, 2),
                                    createRecord("d", 1, 2),
                                    createRecord("e", 1, 2)));
        }
    }

    @Nested
    @DisplayName("Behaves as an iterator")
    class BehavesAsAnIterator {

        @Test
        void shouldReturnTrueForHasNextWithPopulatedIterator() {
            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRecords(
                    List.of(RowFactory.create("a", 1, 2)).iterator());

            // Then
            assertThat(fileWritingIterator).hasNext();
        }

        @Test
        void shouldReturnFalseForHasNextWithEmptyIterator() {
            // Given
            Iterator<Row> empty = Collections.emptyIterator();

            // When
            SingleFileWritingIterator fileWritingIterator = createIteratorOverRecords(empty);

            // Then
            assertThat(fileWritingIterator).isExhausted();
        }
    }

    private SingleFileWritingIterator createIteratorOverRecords(Iterator<Row> records) {
        return createIteratorOverRecordsWithPartitions(records,
                PartitionsFromSplitPoints.treeFrom(schema, List.of("T")));
    }

    private SingleFileWritingIterator createIteratorOverRecordsWithPartitions(
            Iterator<Row> records, PartitionTree partitionTree) {
        return new SingleFileWritingIterator(records,
                instanceProperties, tableProperties,
                new Configuration(), partitionTree);
    }

    private SingleFileWritingIterator createIteratorOverRecordsWithPartitionsAndOutputFilename(
            Iterator<Row> records, PartitionTree partitionTree, String filename) {
        return new SingleFileWritingIterator(records,
                instanceProperties, tableProperties,
                new Configuration(), partitionTree, filename);
    }

    private static InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(UserDefinedInstanceProperty.FILE_SYSTEM, "file://");
        return instanceProperties;
    }

    private static Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("int", new IntType()))
                .valueFields(new Field("value", new IntType()))
                .build();
    }

    private static TableProperties createTableProperties(
            InstanceProperties instanceProperties, Schema schema, java.nio.file.Path tempFolder) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        tableProperties.set(TableProperty.DATA_BUCKET, tempFolder.toString());

        return tableProperties;
    }

    private Record createRecord(Object... values) {
        return createRecord(RowFactory.create(values), schema);
    }

    private Record createRecord(Row row, Schema schema) {
        Record record = new Record();
        int i = 0;
        for (Field field : schema.getAllFields()) {
            record.put(field.getName(), row.get(i));
            i++;
        }
        return record;
    }

    private List<Record> readRecords(String path) {
        try (ParquetRecordReader reader = new ParquetRecordReader(new Path(path), schema)) {
            List<Record> records = new ArrayList<>();
            Record record = reader.read();
            while (null != record) {
                records.add(new Record(record));
                record = reader.read();
            }
            reader.close();
            return records;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String readPartitionIdFromOutputFileMetadata(Row metadataRow) {
        return metadataRow.getString(0);
    }

    private String readPathFromOutputFileMetadata(Row metadataRow) {
        return metadataRow.getString(1);
    }

    private PartitionsBuilder createPartitionsBuilder() {
        return new PartitionsBuilder(schema);
    }
}