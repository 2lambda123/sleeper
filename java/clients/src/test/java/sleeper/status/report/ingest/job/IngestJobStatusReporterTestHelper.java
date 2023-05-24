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

package sleeper.status.report.ingest.job;

import sleeper.ToStringPrintStream;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.status.report.StatusReporterTestHelper;
import sleeper.status.report.job.query.JobQuery;

import java.util.List;
import java.util.stream.Collectors;

public class IngestJobStatusReporterTestHelper {
    private IngestJobStatusReporterTestHelper() {
    }

    public static String replaceBracketedJobIds(List<IngestJobStatus> job, String example) {
        return StatusReporterTestHelper.replaceBracketedJobIds(job.stream()
                .map(IngestJobStatus::getJobId)
                .collect(Collectors.toList()), example);
    }

    public static String getStandardReport(JobQuery.Type query, List<IngestJobStatus> statusList, int numberInQueue) {
        ToStringPrintStream output = new ToStringPrintStream();
        new StandardIngestJobStatusReporter(output.getPrintStream()).report(statusList, query, numberInQueue);
        return output.toString();
    }

    public static String getJsonReport(JobQuery.Type query, List<IngestJobStatus> statusList, int numberInQueue) {
        ToStringPrintStream output = new ToStringPrintStream();
        new JsonIngestJobStatusReporter(output.getPrintStream()).report(statusList, query, numberInQueue);
        return output.toString();
    }
}