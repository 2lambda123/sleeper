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
package sleeper.clients.teardown;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClientBuilder;
import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.PopulateInstanceProperties;
import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.local.LoadLocalProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class TearDownInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(TearDownInstance.class);

    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonCloudWatchEvents cloudWatch;
    private final AmazonECS ecs;
    private final AmazonECR ecr;
    private final AmazonElasticMapReduce emr;
    private final CloudFormationClient cloudFormation;
    private final Path scriptsDir;
    private final Path generatedDir;
    private final String instanceIdArg;
    private final Function<InstanceProperties, List<String>> getExtraEcsClusters;
    private final Function<InstanceProperties, List<String>> getExtraEcrRepositories;

    private TearDownInstance(Builder builder) {
        s3 = Objects.requireNonNull(builder.s3, "s3 must not be null");
        s3v2 = Objects.requireNonNull(builder.s3v2, "s3v2 must not be null");
        cloudWatch = Objects.requireNonNull(builder.cloudWatch, "cloudWatch must not be null");
        ecs = Objects.requireNonNull(builder.ecs, "ecs must not be null");
        ecr = Objects.requireNonNull(builder.ecr, "ecr must not be null");
        emr = Objects.requireNonNull(builder.emr, "emr must not be null");
        cloudFormation = Objects.requireNonNull(builder.cloudFormation, "cloudFormation must not be null");
        scriptsDir = Objects.requireNonNull(builder.scriptsDir, "scriptsDir must not be null");
        getExtraEcsClusters = Objects.requireNonNull(builder.getExtraEcsClusters, "getExtraEcsClusters must not be null");
        getExtraEcrRepositories = Objects.requireNonNull(builder.getExtraEcrRepositories, "getExtraEcrRepositories must not be null");
        instanceIdArg = builder.instanceId;
        generatedDir = scriptsDir.resolve("generated");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Usage: <scripts directory> <optional instance id>");
        }
        builder().scriptsDir(Path.of(args[0]))
                .instanceId(optionalArgument(args, 1).orElse(null))
                .tearDownWithDefaultClients();
    }

    public void tearDown() throws IOException, InterruptedException {
        InstanceProperties instanceProperties = loadInstanceConfig();

        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("Tear Down");
        LOGGER.info("--------------------------------------------------------");
        LOGGER.info("scriptsDir: {}", scriptsDir);
        LOGGER.info("generatedDir: {}", generatedDir);
        LOGGER.info("{}: {}", ID.getPropertyName(), instanceProperties.get(ID));
        LOGGER.info("{}: {}", CONFIG_BUCKET.getPropertyName(), instanceProperties.get(CONFIG_BUCKET));
        LOGGER.info("{}: {}", QUERY_RESULTS_BUCKET.getPropertyName(), instanceProperties.get(QUERY_RESULTS_BUCKET));

        new ShutdownSystemProcesses(cloudWatch, ecs, emr)
                .shutdown(instanceProperties, getExtraEcsClusters.apply(instanceProperties));

        LOGGER.info("Deleting deployed CloudFormation stack");
        try {
            cloudFormation.deleteStack(builder -> builder.stackName(instanceProperties.get(ID)));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed deleting stack", e);
        }

        LOGGER.info("Waiting for CloudFormation stack to delete");
        WaitForStackToDelete.from(cloudFormation, instanceProperties.get(ID)).pollUntilFinished();

        LOGGER.info("Removing the Jars bucket and docker containers");
        RemoveJarsBucket.remove(s3v2, instanceProperties.get(JARS_BUCKET));
        RemoveECRRepositories.remove(ecr, instanceProperties, getExtraEcrRepositories.apply(instanceProperties));

        LOGGER.info("Removing generated files");
        ClientUtils.clearDirectory(generatedDir);

        LOGGER.info("Finished tear down");
    }

    public static Builder builder() {
        return new Builder();
    }

    private InstanceProperties loadInstanceConfig() throws IOException {
        String instanceId;
        if (instanceIdArg == null) {
            InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(generatedDir);
            instanceId = instanceProperties.get(ID);
        } else {
            instanceId = instanceIdArg;
        }
        LOGGER.info("Loading configuration for instance {}", instanceId);
        try {
            InstanceProperties properties = new InstanceProperties();
            properties.loadFromS3GivenInstanceId(s3, instanceId);
            return properties;
        } catch (AmazonS3Exception e) {
            LOGGER.info("Failed to download configuration, using default properties");
            return PopulateInstanceProperties.generateTearDownDefaultsFromInstanceId(instanceId);
        }
    }

    public static final class Builder {
        private AmazonS3 s3;
        private S3Client s3v2;
        private AmazonCloudWatchEvents cloudWatch;
        private AmazonECS ecs;
        private AmazonECR ecr;
        private AmazonElasticMapReduce emr;
        private CloudFormationClient cloudFormation;
        private Path scriptsDir;
        private String instanceId;
        private Function<InstanceProperties, List<String>> getExtraEcsClusters = properties -> List.of();
        private Function<InstanceProperties, List<String>> getExtraEcrRepositories = properties -> List.of();

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3v2(S3Client s3v2) {
            this.s3v2 = s3v2;
            return this;
        }

        public Builder cloudWatch(AmazonCloudWatchEvents cloudWatch) {
            this.cloudWatch = cloudWatch;
            return this;
        }

        public Builder ecs(AmazonECS ecs) {
            this.ecs = ecs;
            return this;
        }

        public Builder ecr(AmazonECR ecr) {
            this.ecr = ecr;
            return this;
        }

        public Builder emr(AmazonElasticMapReduce emr) {
            this.emr = emr;
            return this;
        }

        public Builder cloudFormation(CloudFormationClient cloudFormation) {
            this.cloudFormation = cloudFormation;
            return this;
        }

        public Builder scriptsDir(Path scriptsDir) {
            this.scriptsDir = scriptsDir;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder getExtraEcsClusters(Function<InstanceProperties, List<String>> getExtraEcsClusters) {
            this.getExtraEcsClusters = getExtraEcsClusters;
            return this;
        }

        public Builder getExtraEcrRepositories(Function<InstanceProperties, List<String>> getExtraEcrRepositories) {
            this.getExtraEcrRepositories = getExtraEcrRepositories;
            return this;
        }

        public TearDownInstance build() {
            return new TearDownInstance(this);
        }

        public void tearDownWithDefaultClients() throws IOException, InterruptedException {
            try (S3Client s3v2Client = S3Client.create();
                 CloudFormationClient cloudFormationClient = CloudFormationClient.create()) {
                s3(AmazonS3ClientBuilder.defaultClient());
                s3v2(s3v2Client);
                cloudWatch(AmazonCloudWatchEventsClientBuilder.defaultClient());
                ecs(AmazonECSClientBuilder.defaultClient());
                ecr(AmazonECRClientBuilder.defaultClient());
                emr(AmazonElasticMapReduceClientBuilder.defaultClient());
                cloudFormation(cloudFormationClient);
                build().tearDown();
            }
        }
    }
}
