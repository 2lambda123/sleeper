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
package sleeper.compaction.status.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.After;
import org.junit.Before;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.status.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.status.task.DynamoDBCompactionTaskStatusStoreCreator;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.InstanceProperties;

import java.time.Instant;
import java.util.List;

import static sleeper.compaction.status.task.DynamoDBCompactionTaskStatusStore.taskStatusTableName;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class DynamoDBCompactionTaskStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("startedStatus.startUpdateTime", "finishedStatus.updateTime", "expiryDate").build();
    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String taskStatusTableName = taskStatusTableName(instanceProperties.get(ID));
    protected final CompactionTaskStatusStore store = DynamoDBCompactionTaskStatusStore.from(dynamoDBClient, instanceProperties);

    @Before
    public void setUp() {
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(taskStatusTableName);
    }

    protected static Instant defaultStartTime() {
        return Instant.parse("2022-09-22T12:30:00.000Z");
    }

    protected static Instant defaultFinishTime() {
        return Instant.parse("2022-09-22T16:30:00.000Z");
    }

    protected static CompactionTaskStatus startedTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().build();
    }

    protected static CompactionTaskStatus.Builder startedTaskWithDefaultsBuilder() {
        return CompactionTaskStatus.started(defaultStartTime().toEpochMilli());
    }

    protected static CompactionTaskStatus finishedTaskWithDefaults(List<CompactionJobSummary> jobSummaryList) {
        CompactionTaskStatus.Builder taskStatusBuilder = startedTaskWithDefaultsBuilder();
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        jobSummaryList.forEach(taskFinishedBuilder::addJobSummary);
        return taskStatusBuilder.finished(taskFinishedBuilder, defaultFinishTime().toEpochMilli()).build();
    }

}
