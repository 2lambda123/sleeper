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
package sleeper.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.status.AverageCompactionRate;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AverageCompactionRateTest {

    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    public void shouldCalculateAverageOfSingleFinishedCompactionJob() {
        // Given
        List<CompactionJobStatus> jobs = Collections.singletonList(
                dataHelper.finishedCompactionStatus(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L));

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(1, 10.0, 10.0);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedCompactionJobs() {
        // Given
        List<CompactionJobStatus> jobs = Arrays.asList(
                dataHelper.finishedCompactionStatus(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L), // compaction rate 10/s
                dataHelper.finishedCompactionStatus(
                        Instant.parse("2022-10-13T10:19:00.000Z"),
                        Duration.ofSeconds(10), 50L, 50L)); // compaction rate 5/s

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(2, 7.5, 7.5);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedCompactionJobsWithDifferentDurations() {
        // Given
        List<CompactionJobStatus> jobs = Arrays.asList(
                dataHelper.finishedCompactionStatus(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(100), 1000L, 1000L), // compaction rate 10/s
                dataHelper.finishedCompactionStatus(
                        Instant.parse("2022-10-13T10:19:00.000Z"),
                        Duration.ofSeconds(10), 50L, 50L)); // compaction rate 5/s

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(2, 7.5, 7.5);
    }

    @Test
    public void shouldCalculateAverageOfTwoFinishedCompactionRunsWithDifferentDurations() {
        // Given
        List<CompactionJobStatus> jobs = Collections.singletonList(
                dataHelper.compactionStatusWithJobRunsStartToFinish(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        runs -> runs
                                .finishedRun(Duration.ofSeconds(100), 1000L, 1000L) // compaction rate 10/s
                                .finishedRun(Duration.ofSeconds(10), 50L, 50L))); // compaction rate 5/s

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(2, 7.5, 7.5);
    }

    @Test
    public void shouldIgnoreUnstartedCompactionJob() {
        // Given
        List<CompactionJobStatus> jobs = Arrays.asList(
                dataHelper.finishedCompactionStatus(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L),
                dataHelper.createdCompactionStatus(
                        Instant.parse("2022-10-13T10:19:00.000Z")));

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(1, 10.0, 10.0);
    }

    @Test
    public void shouldIgnoreUnfinishedCompactionJob() {
        // Given
        List<CompactionJobStatus> jobs = Arrays.asList(
                dataHelper.finishedCompactionStatus(
                        Instant.parse("2022-10-13T10:18:00.000Z"),
                        Duration.ofSeconds(10), 100L, 100L),
                dataHelper.startedCompactionStatus(
                        Instant.parse("2022-10-13T10:19:00.000Z")));

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(1, 10.0, 10.0);
    }

    @Test
    public void shouldReportNoCompactionJobs() {
        // Given
        List<CompactionJobStatus> jobs = Collections.emptyList();

        // When / Then
        assertThat(AverageCompactionRate.of(jobs))
                .extracting(
                        AverageCompactionRate::getJobCount,
                        AverageCompactionRate::getRecordsReadPerSecond,
                        AverageCompactionRate::getRecordsWrittenPerSecond)
                .containsExactly(0, Double.NaN, Double.NaN);
    }

}