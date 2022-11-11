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
package sleeper.bulkimport.job.runner.dataframe;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.runner.BulkImportJobRunner;
import sleeper.bulkimport.job.runner.WriteParquetFiles;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE;

/**
 * The {@link BulkImportJobDataframeRunner} is a {@link BulkImportJobRunner} which
 * uses Spark's Dataframe API to efficiently sort and write out the data split by
 * Sleeoer partition.
 */
public class BulkImportJobDataframeRunner extends BulkImportJobRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDataframeRunner.class);

    @Override
    public Dataset<Row> createFileInfos(
            Dataset<Row> rows,
            BulkImportJob job,
            TableProperties tableProperties,
            Broadcast<List<Partition>> broadcastedPartitions, Configuration conf) throws IOException {
        Schema schema = tableProperties.getSchema();

        LOGGER.info("Running bulk import job with id {}", job.getId());
        Column[] sortColumns = Lists.newArrayList(
                        Lists.newArrayList(PARTITION_FIELD_NAME), schema.getRowKeyFieldNames(), schema.getSortKeyFieldNames())
                .stream()
                .flatMap(list -> ((List<String>) list).stream())
                .map(Column::new)
                .collect(Collectors.toList())
                .toArray(new Column[0]);
        LOGGER.info("Sorting by columns {}", String.join(",", Arrays.stream(sortColumns).map(c -> c.toString()).collect(Collectors.toList())));

        int numLeafPartitions = (int) broadcastedPartitions.value().stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("There are {} leaf partitions", numLeafPartitions);

        Dataset<Row> sortedRows = rows.sort(sortColumns);
        LOGGER.info("There are {} partitions in the sorted Dataset", sortedRows.rdd().getNumPartitions());

        int minPartitionsToUseCoalesce = getInstanceProperties().getInt(BULK_IMPORT_MIN_PARTITIONS_TO_USE_COALESCE);
        LOGGER.info("The minimum number of leaf partitions to use coalesce in the bulk import is {}", minPartitionsToUseCoalesce);

        if (numLeafPartitions < minPartitionsToUseCoalesce) {
            LOGGER.info("Not using coalesce");
        } else {
            LOGGER.info("Coalescing data to {} partitions", numLeafPartitions);
            sortedRows = sortedRows.coalesce(numLeafPartitions);
            LOGGER.info("Coalesced data has {} partitions", sortedRows.rdd().getNumPartitions());
        }

        return sortedRows.mapPartitions(new WriteParquetFiles(getInstanceProperties().saveAsString(), tableProperties.saveAsString(), conf), RowEncoder.apply(createFileInfoSchema()));
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobRunner.start(args, new BulkImportJobDataframeRunner());
    }
}
