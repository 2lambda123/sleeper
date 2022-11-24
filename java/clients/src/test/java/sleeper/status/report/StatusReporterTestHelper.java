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

package sleeper.status.report;

import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;

import java.time.Instant;
import java.util.List;
import java.util.function.Function;

import static sleeper.ClientTestUtils.exampleUUID;

public class StatusReporterTestHelper {
    private StatusReporterTestHelper() {
    }

    public static String job(int number) {
        return exampleUUID("job", number);
    }

    public static String task(int number) {
        return exampleUUID("task", number);
    }

    public static ProcessRun jobRunStartedInTask(int taskNumber, String startTimeNoMillis) {
        return ProcessRun.started(task(taskNumber), defaultStartedStatus(startTimeNoMillis));
    }

    public static ProcessRun jobRunFinishedInTask(int taskNumber, String startTimeNoMillis, String finishTimeNoMillis) {
        return ProcessRun.finished(task(taskNumber),
                defaultStartedStatus(startTimeNoMillis),
                defaultFinishedStatus(startTimeNoMillis, finishTimeNoMillis));
    }

    public static String replaceStandardJobIds(List<CompactionJobStatus> jobs, String example) {
        return replaceJobIds(jobs, StatusReporterTestHelper::job, example);
    }

    public static String replaceBracketedJobIds(List<CompactionJobStatus> jobs, String example) {
        return replaceJobIds(jobs, number -> "$(jobId" + number + ")", example);
    }

    protected static String replaceJobIds(
            List<CompactionJobStatus> jobs, Function<Integer, String> getTemplateId, String example) {
        String replaced = example;
        for (int i = 0; i < jobs.size(); i++) {
            replaced = replaced.replace(getTemplateId.apply(i + 1), jobs.get(i).getJobId());
        }
        return replaced;
    }

    public static ProcessStartedStatus defaultStartedStatus(String startTimeNoMillis) {
        return ProcessStartedStatus.updateAndStartTime(
                Instant.parse(startTimeNoMillis + ".123Z"),
                Instant.parse(startTimeNoMillis + ".001Z"));
    }

    public static ProcessFinishedStatus defaultFinishedStatus(String startTimeNoMillis, String finishTimeNoMillis) {
        return ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse(finishTimeNoMillis + ".123Z"),
                new RecordsProcessedSummary(
                        new RecordsProcessed(300L, 200L),
                        Instant.parse(startTimeNoMillis + ".001Z"),
                        Instant.parse(finishTimeNoMillis + ".001Z")));
    }
}
