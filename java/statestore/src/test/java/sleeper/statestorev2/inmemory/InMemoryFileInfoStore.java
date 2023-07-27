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
package sleeper.statestorev2.inmemory;

import sleeper.statestorev2.FileInfo;
import sleeper.statestorev2.FileInfo.FileStatus;
import sleeper.statestorev2.FileInfoStore;
import sleeper.statestorev2.StateStoreException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static sleeper.statestorev2.FileInfo.FileStatus.ACTIVE;
import static sleeper.statestorev2.FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING;

/**
 * This class is intended for testing only. It is not thread-safe and should not be used
 * where concurrent operations may be happening.
 */
public class InMemoryFileInfoStore implements FileInfoStore {
    private final Map<String, Map<String, FileInfo>> fileInPartitionEntries = new HashMap<>(); // filename -> partition id -> fileinfo
    private final Map<String, FileInfo> fileLifecycleEntries = new HashMap<>();
    private final long gcDelayInSeconds;
    private final Supplier<Instant> timeSupplier;

    public InMemoryFileInfoStore(long gcDelayInSeconds) {
        this(gcDelayInSeconds, Instant::now);
    }

    public InMemoryFileInfoStore(long gcDelayInSeconds, Supplier<Instant> timeSupplier) {
        this.gcDelayInSeconds = gcDelayInSeconds;
        this.timeSupplier = timeSupplier;
    }

    public InMemoryFileInfoStore() {
        this(Long.MAX_VALUE, Instant::now);
    }

    @Override
    public void addFile(FileInfo fileInfo) {
        if (null == fileInfo.getFilename()
                || null == fileInfo.getPartitionId()
                || null == fileInfo.getNumberOfRecords()) {
            throw new IllegalArgumentException("FileInfo needs non-null filename, partition, number of records: got " + fileInfo);
        }
        fileInPartitionEntries.putIfAbsent(fileInfo.getFilename(), new HashMap<>());
        fileInPartitionEntries.get(fileInfo.getFilename())
                .put(fileInfo.getPartitionId(), fileInfo.cloneWithStatus(FileStatus.FILE_IN_PARTITION));
        fileLifecycleEntries.put(fileInfo.getFilename(), fileInfo.cloneWithStatus(FileStatus.ACTIVE));
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) {
        fileInfos.forEach(this::addFile);
    }

    @Override
    public synchronized void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(List<FileInfo> fileInPartitionRecordsToBeDeleted,
                                                                                          FileInfo newActiveFile) throws StateStoreException {
        checkAndDeleteFileInPartitionInfos(fileInPartitionRecordsToBeDeleted);
        addFile(newActiveFile);
    }

    @Override
    public synchronized void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(List<FileInfo> fileInPartitionRecordsToBeDeleted,
                                                                                           FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        checkAndDeleteFileInPartitionInfos(fileInPartitionRecordsToBeDeleted);
        addFile(leftFileInfo);
        addFile(rightFileInfo);
    }

    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos) throws StateStoreException {
        List<String> filenamesWithJobId = findFilenamesWithJobIdSet(fileInfos);
        if (!filenamesWithJobId.isEmpty()) {
            throw new StateStoreException("Job ID already set: " + filenamesWithJobId);
        }
        for (FileInfo file : fileInfos) {
            FileInfo temp = fileInPartitionEntries.get(file.getFilename()).get(file.getPartitionId()).toBuilder()
                    .jobId(jobId)
                    .build();
            fileInPartitionEntries.get(file.getFilename()).put(file.getPartitionId(), temp);
        }
    }

    private List<String> findFilenamesWithJobIdSet(List<FileInfo> fileInfos) {
        List<String> filenamesWithJobIdSet = new ArrayList<>();
        for (FileInfo file : fileInfos) {
            if (null != fileInPartitionEntries.get(file.getFilename()).get(file.getPartitionId()).getJobId()) {
                filenamesWithJobIdSet.add(file.getFilename());
            }
        }
        return filenamesWithJobIdSet;
    }

    @Override
    public void deleteFileLifecycleEntries(List<String> filenames) {
        filenames.forEach(this::deleteReadyForGCFile);
    }

    private void deleteReadyForGCFile(String filename) {
        fileLifecycleEntries.remove(filename);
    }

    @Override
    public List<FileInfo> getFileInPartitionList() {
        return fileInPartitionEntries.values().stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public List<FileInfo> getFileLifecycleList() {
        return new ArrayList<>(fileLifecycleEntries.values());
    }

    @Override
    public List<FileInfo> getActiveFileList() {
        return fileLifecycleEntries.values().stream()
                .filter(f -> f.getFileStatus().equals(ACTIVE))
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<String> getReadyForGCFiles() {
        return getReadyForGCFileInfoStream().map(FileInfo::getFilename).iterator();
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFileInfos() {
        return getReadyForGCFileInfoStream().iterator();
    }

    private Stream<FileInfo> getReadyForGCFileInfoStream() {
        long delayInMilliseconds = 1000L * gcDelayInSeconds;
        long deleteTime = timeSupplier.get().toEpochMilli() - delayInMilliseconds;
        return fileLifecycleEntries.values().stream()
                .filter(f -> f.getFileStatus().equals(GARBAGE_COLLECTION_PENDING))
                .filter(f -> (f.getLastStateStoreUpdateTime() < deleteTime));
    }

    @Override
    public void findFilesThatShouldHaveStatusOfGCPending() {
        // Identify any files which have lifecycle records that do not have any
        // file-in-partition records.
        Set<String> allFilesWithLifecycleEntries = new HashSet<>(fileLifecycleEntries.keySet());
        Set<String> allFilesWithFileInPartitionEntries = new HashSet<>(fileInPartitionEntries.keySet());
        allFilesWithLifecycleEntries.removeAll(allFilesWithFileInPartitionEntries);
        // Update the file-lifecylce records to GARBAGE_COLLECTION_PENDING.
        for (String filename : allFilesWithLifecycleEntries) {
            FileInfo updatedFileInfo = fileLifecycleEntries.get(filename).cloneWithStatus(FileStatus.GARBAGE_COLLECTION_PENDING);
            fileLifecycleEntries.put(filename, updatedFileInfo);
        }
    }

    @Override
    public List<FileInfo> getFileInPartitionInfosWithNoJobId() {
        return getFileInPartitionList().stream()
                .filter(file -> file.getJobId() == null)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, List<String>> getPartitionToFileInPartitionMap() {
        return getFileInPartitionList().stream().collect(
                groupingBy(FileInfo::getPartitionId,
                        mapping(FileInfo::getFilename, Collectors.toList())));
    }

    private synchronized void checkAndDeleteFileInPartitionInfos(List<FileInfo> fileInPartitionRecordsToBeDeleted)
            throws StateStoreException {
        // Check that the right file-in-partition records exist
        List<FileInfo> failures = new ArrayList<>();
        for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
            if (!fileInPartitionEntries.containsKey(fileInfo.getFilename())) {
                failures.add(fileInfo);
            } else {
                Map<String, FileInfo> partitionToFileInfo = fileInPartitionEntries.get(fileInfo.getFilename());
                if (!partitionToFileInfo.containsKey(fileInfo.getPartitionId())) {
                    failures.add(fileInfo);
                }
            }
        }
        if (!failures.isEmpty()) {
            throw new StateStoreException("Some of the provided fileInPartitionRecordsToBeDeleted do not have the correct"
                    + " file-in-partition entries" + failures);
        }

        // Delete the records
        for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
            fileInPartitionEntries.get(fileInfo.getFilename()).remove(fileInfo.getPartitionId());
            if (fileInPartitionEntries.get(fileInfo.getFilename()).isEmpty()) {
                fileInPartitionEntries.remove(fileInfo.getFilename());
            }
        }
    }

    @Override
    public void initialise() throws StateStoreException {
    }
}
