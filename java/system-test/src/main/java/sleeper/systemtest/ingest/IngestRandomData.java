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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.SystemTestProperties;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;

import static sleeper.systemtest.SystemTestProperty.INGEST_MODE;

/**
 * Entrypoint for SystemTest image. Writes random data to Sleeper using the mechanism (ingestMode) defined in
 * the properties which were written to S3.
 */
public class IngestRandomData {

    private IngestRandomData() {
    }

    public static void main(String[] args) throws IOException, ObjectFactoryException {
        if (args.length != 2) {
            throw new RuntimeException("Wrong number of arguments detected. Usage: IngestRandomData <S3 bucket> <Table name>");
        }
        String s3Bucket = args[0];
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3(s3Client, instanceProperties);

        ObjectFactory objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");

        TableProperties tableProperties = new TablePropertiesProvider(s3Client, instanceProperties).getTableProperties(args[1]);

        StateStore stateStore = new StateStoreProvider(dynamoDBClient, instanceProperties, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties))
                .getStateStore(tableProperties);

        s3Client.shutdown();

        String ingestMode = systemTestProperties.get(INGEST_MODE);
        if (IngestMode.QUEUE.name().equalsIgnoreCase(ingestMode) || IngestMode.BULK_IMPORT_QUEUE.name().equalsIgnoreCase(ingestMode)) {
            new WriteRandomDataViaQueueJob(ingestMode, objectFactory,
                    instanceProperties, tableProperties, systemTestProperties, stateStore).run();
        } else if (IngestMode.DIRECT.name().equalsIgnoreCase(ingestMode)) {
            new UploadMultipleShardedSortedParquetFiles(objectFactory,
                    instanceProperties, tableProperties, systemTestProperties, stateStore).run();
        } else {
            throw new IllegalArgumentException("Unrecognised ingest mode: " + ingestMode +
                    ". Only direct and queue ingest modes are available.");
        }
    }
}
