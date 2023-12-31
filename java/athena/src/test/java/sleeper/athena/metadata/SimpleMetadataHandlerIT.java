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
package sleeper.athena.metadata;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.junit.jupiter.api.Test;

import sleeper.athena.TestUtils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public class SimpleMetadataHandlerIT extends AbstractMetadataHandlerIT {

    @Test
    public void shouldCreateSplitForEachFileInAPartition() throws Exception {
        // Given
        InstanceProperties instance = TestUtils.createInstance(createS3Client());
        SimpleMetadataHandler simpleMetadataHandler = new SimpleMetadataHandler(createS3Client(), createDynamoClient(),
                instance.get(CONFIG_BUCKET), mock(EncryptionKeyFactory.class), mock(AWSSecretsManager.class),
                mock(AmazonAthena.class), "abc", "def");

        // When
        Block partitionsBlock = createPartitionsBlock("[ \"a/b/c.parquet\", \"d/e/f.parquet\"]");
        GetSplitsResponse getSplitsResponse = simpleMetadataHandler.doGetSplits(new BlockAllocatorImpl(), new GetSplitsRequest(TestUtils.createIdentity(),
                "abc", "def", new TableName("mydb", "myTable"), partitionsBlock, new ArrayList<>(),
                new Constraints(new HashMap<>()), "continue"));

        // Then
        assertThat(getSplitsResponse.getSplits())
                .extracting(split -> split.getProperties().get(RELEVANT_FILES_FIELD))
                .containsExactlyInAnyOrder("a/b/c.parquet", "d/e/f.parquet");
    }

    @Test
    public void shouldOnlyCreateOneSplitForEachFileAcrossMultiplePartitions() throws Exception {
        // Given
        InstanceProperties instance = TestUtils.createInstance(createS3Client());
        SimpleMetadataHandler simpleMetadataHandler = new SimpleMetadataHandler(createS3Client(), createDynamoClient(),
                instance.get(CONFIG_BUCKET), mock(EncryptionKeyFactory.class), mock(AWSSecretsManager.class),
                mock(AmazonAthena.class), "abc", "def");

        // When
        Block partitionsBlock = createPartitionsBlock("[ \"a/b/c.parquet\", \"d/e/f.parquet\"]",
                "[ \"g/h/i.parquet\", \"d/e/f.parquet\"]");
        GetSplitsResponse getSplitsResponse = simpleMetadataHandler.doGetSplits(new BlockAllocatorImpl(), new GetSplitsRequest(TestUtils.createIdentity(),
                "abc", "def", new TableName("mydb", "myTable"), partitionsBlock, new ArrayList<>(),
                new Constraints(new HashMap<>()), "continue"));

        // Then
        assertThat(getSplitsResponse.getSplits())
                .extracting(split -> split.getProperties().get(RELEVANT_FILES_FIELD))
                .containsExactlyInAnyOrder("a/b/c.parquet", "d/e/f.parquet", "g/h/i.parquet");
    }

    private Block createPartitionsBlock(String... jsonSerialisedLists) {
        BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl();
        Block block = blockAllocator.createBlock(new SchemaBuilder().addStringField(RELEVANT_FILES_FIELD).build());
        block.setRowCount(jsonSerialisedLists.length);
        for (int i = 0; i < jsonSerialisedLists.length; i++) {
            block.setValue(RELEVANT_FILES_FIELD, i, jsonSerialisedLists[i]);
        }

        return block;
    }
}
