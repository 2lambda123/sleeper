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
package sleeper.systemtest.cdk;

import sleeper.cdk.Utils;
import sleeper.cdk.stack.DynamoDBStateStorePermissions;
import sleeper.cdk.stack.S3StateStorePermissions;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;
import sleeper.systemtest.SystemTestProperties;
import sleeper.systemtest.SystemTestProperty;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.AwsLogDriver;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.amazon.awscdk.services.sqs.QueueAttributes;
import software.constructs.Construct;

import java.util.Locale;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_TASK_CPU;
import static sleeper.systemtest.SystemTestProperty.SYSTEM_TEST_TASK_MEMORY;
import static sleeper.systemtest.SystemTestProperty.WRITE_DATA_TASK_DEFINITION_FAMILY;

/**
 * A {@link Stack} to deploy the system test components.
 */
public class SystemTestStack extends NestedStack {
    public static final String SYSTEM_TEST_CLUSTER_NAME = "systemTestClusterName";
    public static final String SYSTEM_TEST_TASK_DEFINITION_FAMILY = "systemTestTaskDefinitionFamily";
    public static final String SYSTEM_TEST_CONTAINER = "SystemTestContainer";

    public SystemTestStack(Construct scope,
                           String id,
                           InstanceProperties instanceProperties,
                           TableProperties tableProperties,
                           SystemTestProperties systemTestProperties) {
        super(scope, id);

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));

        // ECS cluster for tasks to write data
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC2", vpcLookupOptions);
        String clusterName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "system-test-cluster"));
        Cluster cluster = Cluster.Builder
                .create(this, "SystemTestCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        systemTestProperties.set(SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME, cluster.getClusterName());
        CfnOutputProps writeClusterOutputProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, SYSTEM_TEST_CLUSTER_NAME, writeClusterOutputProps);

        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder
                .create(this, "SystemTestTaskDefinition")
                .family(instanceProperties.get(ID) + "SystemTestTaskFamily")
                .cpu(systemTestProperties.getInt(SYSTEM_TEST_TASK_CPU))
                .memoryLimitMiB(systemTestProperties.getInt(SYSTEM_TEST_TASK_MEMORY))
                .build();
        systemTestProperties.set(WRITE_DATA_TASK_DEFINITION_FAMILY, taskDefinition.getFamily());
        CfnOutputProps taskDefinitionFamilyOutputProps = new CfnOutputProps.Builder()
                .value(taskDefinition.getFamily())
                .build();
        new CfnOutput(this, SYSTEM_TEST_TASK_DEFINITION_FAMILY, taskDefinitionFamilyOutputProps);

        IRepository repository = Repository.fromRepositoryName(this, "ECR3", systemTestProperties.get(SYSTEM_TEST_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        AwsLogDriverProps logDriverProps = AwsLogDriverProps.builder()
                .streamPrefix(instanceProperties.get(ID) + "-SystemTestTasks")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();
        LogDriver logDriver = AwsLogDriver.awsLogs(logDriverProps);

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(logDriver)
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .build();
        taskDefinition.addContainer(SYSTEM_TEST_CONTAINER, containerDefinitionOptions);

        configBucket.grantRead(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
        getTableDataBucket(tableProperties).grantReadWrite(taskDefinition.getTaskRole());
        StateStoreStack stateStore = getStateStore(tableProperties);
        stateStore.grantReadActiveFileMetadata(taskDefinition.getTaskRole());
        stateStore.grantReadPartitionMetadata(taskDefinition.getTaskRole());
        String ingestJobQueueUrl = instanceProperties.get(INGEST_JOB_QUEUE_URL);
        if (null != ingestJobQueueUrl) {
            IQueue queue = Queue.fromQueueAttributes(this, "IngestJobQueue",
                    QueueAttributes.builder().queueUrl(ingestJobQueueUrl).build());
            queue.grantSendMessages(taskDefinition.getTaskRole());
        }
        String emrBulkImportJobQueueUrl = instanceProperties.get(BULK_IMPORT_EMR_JOB_QUEUE_URL);
        if (null != emrBulkImportJobQueueUrl) {
            IQueue queue = Queue.fromQueueAttributes(this, "EMRBulkImportJobQueue",
                    QueueAttributes.builder().queueUrl(emrBulkImportJobQueueUrl).build());
            queue.grantSendMessages(taskDefinition.getTaskRole());
        }
    }

    private IBucket getTableDataBucket(TableProperties tableProperties) {
        return Bucket.fromBucketName(this, "TableDataBucket", tableProperties.get(DATA_BUCKET));
    }

    private StateStoreStack getStateStore(TableProperties tableProperties) {
        Class<?> stateStoreClass;
        try {
            stateStoreClass = Class.forName(tableProperties.get(STATESTORE_CLASSNAME));
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find specified state store class", e);
        }
        if (stateStoreClass == DynamoDBStateStore.class) {
            return DynamoDBStateStorePermissions.builder()
                    .activeFileInfoTable(Table.fromTableName(this, "TableActiveFiles",
                            tableProperties.get(ACTIVE_FILEINFO_TABLENAME)))
                    .readyForGCFileInfoTable(Table.fromTableName(this, "TableReadyForGCFiles",
                            tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME)))
                    .partitionTable(Table.fromTableName(this, "TablePartitions",
                            tableProperties.get(PARTITION_TABLENAME)))
                    .build();
        } else if (stateStoreClass == S3StateStore.class) {
            return S3StateStorePermissions.builder()
                    .revisionTable(Table.fromTableName(this, "TableRevisions",
                            tableProperties.get(REVISION_TABLENAME)))
                    .dataBucket(getTableDataBucket(tableProperties))
                    .build();
        } else {
            throw new IllegalArgumentException("Could not find state store by class: " + stateStoreClass.getName());
        }
    }
}
