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
package sleeper.bulkimport.job.runner;

import sleeper.statestore.FileInfo;

import java.util.List;

public class BulkImportJobOutput {

    private final List<FileInfo> fileInfos;
    private final Runnable stopSparkContext;
    private final long numRecords;

    public BulkImportJobOutput(List<FileInfo> fileInfos, Runnable stopSparkContext) {
        this.fileInfos = fileInfos;
        this.stopSparkContext = stopSparkContext;
        this.numRecords = fileInfos.stream()
                .mapToLong(FileInfo::getNumberOfRecords)
                .sum();
    }

    public List<FileInfo> fileInfos() {
        return fileInfos;
    }

    public int numFiles() {
        return fileInfos.size();
    }

    public long numRecords() {
        return numRecords;
    }

    public void stopSparkContext() {
        stopSparkContext.run();
    }
}