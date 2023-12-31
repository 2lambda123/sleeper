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

package sleeper.systemtest.drivers.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.job.common.QueueMessageCount;

import java.util.Objects;
import java.util.function.Predicate;

public class WaitForQueueEstimate {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForQueueEstimate.class);

    private final QueueMessageCount.Client queueClient;
    private final Predicate<QueueMessageCount> isFinished;
    private final String description;
    private final String queueUrl;
    private final PollWithRetries poll;

    public static WaitForQueueEstimate notEmpty(
            QueueMessageCount.Client queueClient, InstanceProperties instanceProperties, InstanceProperty queueProperty,
            PollWithRetries poll) {
        String queueUrl = instanceProperties.get(queueProperty);
        return new WaitForQueueEstimate(queueClient, queueUrl,
                estimate -> estimate.getApproximateNumberOfMessages() > 0,
                "estimate not empty for queue " + queueUrl, poll);
    }

    public static WaitForQueueEstimate isEmpty(
            QueueMessageCount.Client queueClient,
            InstanceProperties instanceProperties, InstanceProperty queueProperty, PollWithRetries poll) {
        String queueUrl = instanceProperties.get(queueProperty);
        return new WaitForQueueEstimate(queueClient, queueUrl,
                estimate -> estimate.getApproximateNumberOfMessages() == 0,
                "estimate empty for queue " + queueUrl, poll);
    }

    public static WaitForQueueEstimate isConsumed(
            QueueMessageCount.Client queueClient,
            InstanceProperties instanceProperties, InstanceProperty queueProperty, PollWithRetries poll) {
        String queueUrl = instanceProperties.get(queueProperty);
        return new WaitForQueueEstimate(queueClient, queueUrl,
                estimate -> estimate.getApproximateNumberOfMessages() == 0
                        && estimate.getApproximateNumberOfMessagesNotVisible() == 0,
                "estimate fully consumed for queue " + queueUrl, poll);
    }

    public static WaitForQueueEstimate matchesUnstartedJobs(
            QueueMessageCount.Client queueClient, InstanceProperties instanceProperties, InstanceProperty queueProperty,
            CompactionJobStatusStore statusStore, String tableName, PollWithRetries poll) {
        return new WaitForQueueEstimate(queueClient, instanceProperties, queueProperty,
                estimate -> {
                    long unstarted = statusStore.getUnstartedJobs(tableName).size();
                    LOGGER.info("Found {} unstarted compaction jobs", unstarted);
                    return estimate.getApproximateNumberOfMessages() >= unstarted;
                },
                "queue estimate matching unstarted compaction jobs", poll);
    }

    private WaitForQueueEstimate(QueueMessageCount.Client queueClient,
                                 InstanceProperties properties, InstanceProperty queueProperty,
                                 Predicate<QueueMessageCount> isFinished, String description,
                                 PollWithRetries poll) {
        this(queueClient, properties.get(queueProperty), isFinished, description, poll);
    }

    private WaitForQueueEstimate(QueueMessageCount.Client queueClient, String queueUrl,
                                 Predicate<QueueMessageCount> isFinished, String description,
                                 PollWithRetries poll) {
        this.queueClient = Objects.requireNonNull(queueClient, "queueClient must not be null");
        this.queueUrl = Objects.requireNonNull(queueUrl, "queueUrl must not be null");
        this.isFinished = Objects.requireNonNull(isFinished, "isFinished must not be null");
        this.description = Objects.requireNonNull(description, "description must not be null");
        this.poll = Objects.requireNonNull(poll, "poll must not be null");
    }

    public void pollUntilFinished() throws InterruptedException {
        LOGGER.info("Waiting until {}", description);
        poll.pollUntil(description, this::isFinished);
    }

    private boolean isFinished() {
        QueueMessageCount count = queueClient.getQueueMessageCount(queueUrl);
        LOGGER.info("Message count for queue {}: {}", queueUrl, count);
        return isFinished.test(count);
    }
}
