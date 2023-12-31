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
package sleeper.ingest.batcher.testutil;

import sleeper.ingest.batcher.FileIngestRequest;

import java.util.Objects;

public class IngestBatcherStateStoreKeyFields {

    private final String file;
    private final String tableName;
    private final String jobId;

    private IngestBatcherStateStoreKeyFields(FileIngestRequest request) {
        file = request.getFile();
        tableName = request.getTableName();
        jobId = request.getJobId();
    }

    public static IngestBatcherStateStoreKeyFields keyFor(FileIngestRequest request) {
        return new IngestBatcherStateStoreKeyFields(request);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IngestBatcherStateStoreKeyFields that = (IngestBatcherStateStoreKeyFields) o;

        if (!file.equals(that.file)) {
            return false;
        }
        if (!tableName.equals(that.tableName)) {
            return false;
        }
        return Objects.equals(jobId, that.jobId);
    }

    @Override
    public int hashCode() {
        int result = file.hashCode();
        result = 31 * result + tableName.hashCode();
        result = 31 * result + (jobId != null ? jobId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IngestBatcherStateStoreKeyFields{" +
                "filePath='" + file + '\'' +
                ", tableName='" + tableName + '\'' +
                ", jobId='" + jobId + '\'' +
                '}';
    }
}
