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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.athena.FilterTranslator;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;

/**
 * The {@link SimpleRecordHandler} is an implementation of the {@link SleeperRecordHandler} which takes a single Parquet
 * file provided by the {@link sleeper.athena.metadata.SimpleMetadataHandler} and reads the relevant values using
 * Parquet's predicate pushdown. Unlike the {@link IteratorApplyingRecordHandler} this handler does not apply iterators
 * to the results, however results will likely be returned faster as a consequence.
 */
public class SimpleRecordHandler extends SleeperRecordHandler {
    public SimpleRecordHandler() throws IOException {
        super();
    }

    public SimpleRecordHandler(AmazonS3 s3Client, String configBucket, AWSSecretsManager secretsManager, AmazonAthena athena) throws IOException {
        super(s3Client, configBucket, secretsManager, athena);
    }

    /**
     * Trims down the schema to one derived from the original schema but only containing the fields that require
     * projection (either for filtering, transform or being returned).
     *
     * @param originalSchema the original table schema
     * @param recordsRequest the data request
     * @return an adapted schema only containing fields requested or filtered upon
     */
    @Override
    protected Schema createSchemaForDataRead(Schema originalSchema, ReadRecordsRequest recordsRequest) {
        Set<String> requestedFieldNames = recordsRequest.getSchema().getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toSet());

        return Schema.builder()
                .rowKeyFields(getRelevantFields(originalSchema.getRowKeyFields(), requestedFieldNames))
                .sortKeyFields(getRelevantFields(originalSchema.getSortKeyFields(), requestedFieldNames))
                .valueFields(getRelevantFields(originalSchema.getValueFields(), requestedFieldNames))
                .build();
    }

    private List<sleeper.core.schema.Field> getRelevantFields(List<sleeper.core.schema.Field> originalFields, Set<String> requestedFieldNames) {
        return originalFields.stream()
                .filter(field -> requestedFieldNames.contains(field.getName()))
                .collect(Collectors.toList());

    }

    /**
     * Creates a single parquet iterator from the schema and split, using the constraints to add parquet filters for efficiency.
     *
     * @param recordsRequest  the request
     * @param schema          the schema for reading the data
     * @param tableProperties the table properties for this table
     * @return a Parquet iterator for this split
     * @throws Exception if something goes wrong with the read
     */
    @Override
    protected CloseableIterator<Record> createRecordIterator(ReadRecordsRequest recordsRequest, Schema schema, TableProperties tableProperties) throws Exception {
        String fileName = recordsRequest.getSplit().getProperty(RELEVANT_FILES_FIELD);

        FilterTranslator filterTranslator = new FilterTranslator(schema);
        FilterPredicate filterPredicate = filterTranslator.toPredicate(recordsRequest.getConstraints().getSummary());

        ParquetReader.Builder<Record> recordReaderBuilder = new ParquetRecordReader.Builder(new Path(fileName), schema)
                .withConf(getConfigurationForTable(tableProperties));

        if (filterPredicate != null) {
            recordReaderBuilder.withFilter(FilterCompat.get(filterPredicate));
        }

        return new ParquetReaderIterator(recordReaderBuilder.build());
    }
}
