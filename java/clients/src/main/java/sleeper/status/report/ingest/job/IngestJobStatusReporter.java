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

package sleeper.status.report.ingest.job;

import sleeper.ingest.job.status.IngestJobStatus;

import java.util.List;

public interface IngestJobStatusReporter {
    enum QueryType {
        ALL,
        DETAILED,
        UNFINISHED;

        boolean isParametersRequired() {
            return this == DETAILED;
        }
    }

    void report(List<IngestJobStatus> jobStatusList, QueryType queryType, int numberInQueue);
}
