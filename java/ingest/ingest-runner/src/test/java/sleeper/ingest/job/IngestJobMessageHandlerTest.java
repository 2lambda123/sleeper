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

package sleeper.ingest.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.rejectedRun;

public class IngestJobMessageHandlerTest {
    @Nested
    @DisplayName("Report validation failures")
    class ReportValidationFailures {
        @Test
        void shouldReportValidationFailureIfJsonInvalid() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler ingestJobMessageHandler = new IngestJobMessageHandler(
                    null, new InstanceProperties(), ingestJobStatusStore,
                    () -> "test-job-id", () -> validationTime);
            String json = "{";

            // When
            ingestJobMessageHandler.handleMessage(json);

            // Then
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Error parsing JSON. Reason: End of input at line 1 column 2 path $.")));
        }

        @Test
        void shouldReportModelValidationFailureIfTableNameNotProvided() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler ingestJobMessageHandler = new IngestJobMessageHandler(
                    null, new InstanceProperties(), ingestJobStatusStore,
                    () -> "test-job-id", () -> validationTime);
            String json = "{" +
                    "\"files\":[]" +
                    "}";

            // When
            ingestJobMessageHandler.handleMessage(json);

            // Then
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Model validation failed. Missing property \"tableName\"")));
        }

        @Test
        void shouldReportModelValidationFailureIfFilesNotProvided() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler ingestJobMessageHandler = new IngestJobMessageHandler(
                    null, new InstanceProperties(), ingestJobStatusStore,
                    () -> "test-job-id", () -> validationTime);
            String json = "{" +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            ingestJobMessageHandler.handleMessage(json);

            // Then
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Model validation failed. Missing property \"files\"")));
        }

        @Test
        void shouldReportMultipleModelValidationFailures() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler ingestJobMessageHandler = new IngestJobMessageHandler(
                    null, new InstanceProperties(), ingestJobStatusStore,
                    () -> "test-job-id", () -> validationTime);
            String json = "{}";

            // When
            ingestJobMessageHandler.handleMessage(json);

            // Then
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Model validation failed. Missing property \"files\"",
                                    "Model validation failed. Missing property \"tableName\"")));
        }
    }
}
