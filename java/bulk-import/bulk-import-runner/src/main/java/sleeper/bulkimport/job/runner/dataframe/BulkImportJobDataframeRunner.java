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
package sleeper.bulkimport.job.runner.dataframe;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.runner.BulkImportJobRunner;
import sleeper.bulkimport.job.runner.SparkPartitionRequest;
import sleeper.bulkimport.job.runner.StructTypeFactory;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.bulkimport.job.runner.BulkImportJobRunner.PARTITION_FIELD_NAME;
import static sleeper.bulkimport.job.runner.BulkImportJobRunner.createFileInfoSchema;

/**
 * The {@link BulkImportJobDataframeRunner} is a {@link BulkImportJobRunner} which
 * uses Spark's Dataframe API to efficiently sort and write out the data split by
 * Sleeoer partition.
 */
public class BulkImportJobDataframeRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDataframeRunner.class);

    private BulkImportJobDataframeRunner() {
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobRunner.start(args, BulkImportJobDataframeRunner::createFileInfos);
    }

    public static Dataset<Row> createFileInfos(SparkPartitionRequest request) throws IOException {
        Schema schema = request.schema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        StructType convertedSchema = new StructTypeFactory().getStructType(schema);
        StructType schemaWithPartitionField = createEnhancedSchema(convertedSchema);

        int numLeafPartitions = (int) request.broadcastedPartitions().value()
                .stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("There are {} leaf partitions", numLeafPartitions);

        Dataset<Row> dataWithPartition = request.rows().mapPartitions(
                new AddPartitionFunction(schemaAsString, request.broadcastedPartitions()),
                RowEncoder.apply(schemaWithPartitionField));

        Column[] sortColumns = Lists.newArrayList(
                        Lists.newArrayList(PARTITION_FIELD_NAME),
                        schema.getRowKeyFieldNames(), schema.getSortKeyFieldNames())
                .stream()
                .flatMap(List::stream)
                .map(Column::new)
                .toArray(Column[]::new);
        LOGGER.info("Sorting by columns {}", Arrays.stream(sortColumns)
                .map(Column::toString).collect(Collectors.joining(",")));

        Dataset<Row> sortedRows = dataWithPartition.sort(sortColumns);

        return sortedRows.mapPartitions(
                new WriteParquetFiles(
                        request.instanceProperties().saveAsString(),
                        request.tableProperties().saveAsString(),
                        request.conf()),
                RowEncoder.apply(createFileInfoSchema()));
    }

    private static StructType createEnhancedSchema(StructType convertedSchema) {
        StructType structTypeWithPartition = new StructType(convertedSchema.fields());
        return structTypeWithPartition
                .add(new StructField(PARTITION_FIELD_NAME, DataTypes.StringType, false, null));
    }
}
