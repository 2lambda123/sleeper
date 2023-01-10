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
package sleeper.clients.admin.testutils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.clients.AdminClient;
import sleeper.clients.admin.AdminConfigStore;
import sleeper.core.CommonTestConstants;

@Testcontainers
public abstract class AdminClientITBase extends AdminClientTestBase {

    private static final String CONFIG_BUCKET_NAME = "sleeper-" + INSTANCE_ID + "-config";

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
            .withCredentials(localStackContainer.getDefaultCredentialsProvider())
            .build();

    protected String runClientGetOutput() {
        return runClientGetOutput(client());
    }

    protected AdminClient client() {
        return new AdminClient(store(), out.consoleOut(), in.consoleIn());
    }

    protected AdminConfigStore store() {
        return new AdminConfigStore(s3);
    }

    @BeforeEach
    public void setUp() {
        s3.createBucket(CONFIG_BUCKET_NAME);
    }

    @AfterEach
    public void tearDown() {
        s3.shutdown();
    }

}
