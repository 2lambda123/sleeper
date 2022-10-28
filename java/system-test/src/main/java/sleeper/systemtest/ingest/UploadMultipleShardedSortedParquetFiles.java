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
package sleeper.systemtest.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.IngestRecordsUsingPropertiesSpecifiedMethod;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;

/**
 * Runs {@link sleeper.ingest.IngestRecordsFromIterator} to write random data.
 */
public class UploadMultipleShardedSortedParquetFiles extends WriteRandomDataJob {

    public UploadMultipleShardedSortedParquetFiles(
            ObjectFactory objectFactory,
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            SystemTestProperties testProperties,
            StateStore stateStore) {
        super(objectFactory, instanceProperties, tableProperties, testProperties, stateStore);
    }

    public void run() throws IOException {
        Schema schema = getTableProperties().getSchema();

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        CloseableIterator<Record> recordIterator = new WrappedIterator<>(createRecordIterator(schema));

        try {
            IngestRecordsUsingPropertiesSpecifiedMethod.ingestFromRecordIterator(
                    getClassFactory(),
                    getStateStore(),
                    getInstanceProperties(),
                    getTableProperties(),
                    "/mnt/scratch",
                    null,
                    null,
                    null,
                    null,
                    null,
                    recordIterator);
        } catch (StateStoreException | IteratorException e) {
            throw new IOException("Failed to write records using iterator", e);
        }

        dynamoDBClient.shutdown();
    }
}
