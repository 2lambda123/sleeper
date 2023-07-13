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

package sleeper.clients.deploy;

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SleeperScheduleRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.requireNonEmpty;
import static sleeper.configuration.properties.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.CommonProperty.ID;
import static sleeper.configuration.properties.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.CommonProperty.REGION;
import static sleeper.configuration.properties.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.IngestProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.InstanceProperties.getConfigBucketFromInstanceId;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class PopulateInstanceProperties {
    private final Supplier<String> accountSupplier;
    private final AwsRegionProvider regionProvider;
    private final String instanceId;
    private final String vpcId;
    private final String subnetIds;
    private final InstanceProperties properties;
    private final Properties tagsProperties;

    private PopulateInstanceProperties(Builder builder) {
        accountSupplier = requireNonNull(builder.accountSupplier, "accountSupplier must not be null");
        regionProvider = requireNonNull(builder.regionProvider, "regionProvider must not be null");
        instanceId = requireNonEmpty(builder.instanceId, "instanceId must not be empty");
        vpcId = requireNonEmpty(builder.vpcId, "vpcId must not be empty");
        subnetIds = requireNonEmpty(builder.subnetIds, "subnetIds must not be empty");
        properties = Optional.ofNullable(builder.properties).orElseGet(InstanceProperties::new);
        tagsProperties = Optional.ofNullable(builder.tagsProperties).orElseGet(properties::getTagsProperties);
    }

    public static Builder builder() {
        return new Builder();
    }

    public InstanceProperties populate() {
        InstanceProperties instanceProperties = populateDefaultsFromInstanceId(properties, instanceId);
        tagsProperties.setProperty("InstanceID", instanceId);
        instanceProperties.loadTags(tagsProperties);
        instanceProperties.set(ACCOUNT, accountSupplier.get());
        instanceProperties.set(REGION, regionProvider.getRegion().id());
        instanceProperties.set(VPC_ID, vpcId);
        instanceProperties.set(SUBNETS, subnetIds);
        return instanceProperties;
    }

    public static InstanceProperties generateTearDownDefaultsFromInstanceId(String instanceId) {
        InstanceProperties instanceProperties = populateDefaultsFromInstanceId(new InstanceProperties(), instanceId);
        SleeperScheduleRule.getCloudWatchRuleDefaults(instanceId)
                .forEach(rule -> instanceProperties.set(rule.getProperty(), rule.getPropertyValue()));
        return instanceProperties;
    }

    public static InstanceProperties populateDefaultsFromInstanceId(InstanceProperties properties, String instanceId) {
        properties.set(ID, instanceId);
        properties.set(CONFIG_BUCKET, getConfigBucketFromInstanceId(instanceId));
        properties.set(JARS_BUCKET, String.format("sleeper-%s-jars", instanceId));
        properties.set(QUERY_RESULTS_BUCKET, String.format("sleeper-%s-query-results", instanceId));
        properties.set(ECR_COMPACTION_REPO, instanceId + "/compaction-job-execution");
        properties.set(ECR_INGEST_REPO, instanceId + "/ingest");
        properties.set(BULK_IMPORT_REPO, instanceId + "/bulk-import-runner");
        return properties;
    }

    public static final class Builder {
        private Supplier<String> accountSupplier;
        private AwsRegionProvider regionProvider;
        private String instanceId;
        private String vpcId;
        private String subnetIds;
        private InstanceProperties properties;
        private Properties tagsProperties;

        private Builder() {
        }

        public Builder sts(AWSSecurityTokenService sts) {
            return accountSupplier(sts.getCallerIdentity(new GetCallerIdentityRequest())::getAccount);
        }

        public Builder accountSupplier(Supplier<String> accountSupplier) {
            this.accountSupplier = accountSupplier;
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetIds(String subnetIds) {
            this.subnetIds = subnetIds;
            return this;
        }

        public Builder instanceProperties(InstanceProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder instanceProperties(Path propertiesPath) {
            this.properties = new InstanceProperties();
            try {
                this.properties.load(propertiesPath);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return this;
        }

        public Builder tagsProperties(Properties tagsProperties) {
            this.tagsProperties = tagsProperties;
            return this;
        }

        public Builder deployInstanceConfig(DeployInstanceConfiguration deployInstanceConfig) {
            return instanceProperties(deployInstanceConfig.getInstanceProperties())
                    .tagsProperties(deployInstanceConfig.getInstanceProperties().getTagsProperties());
        }

        public PopulateInstanceProperties build() {
            return new PopulateInstanceProperties(this);
        }
    }
}
