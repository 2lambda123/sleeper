```java
interface IdenticalSignatures {
    void addFile(FileInfo fileInfo) throws StateStoreException;

    void addFiles(List<FileInfo> fileInfos) throws StateStoreException;

    void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> fileInfos)
            throws StateStoreException; // compaction creation

    void initialise() throws StateStoreException;
}

interface CompactionBefore {
    // end of standard compaction
    void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(
            List<FileInfo> filesToBeMarkedReadyForGC,
            FileInfo newActiveFile) throws StateStoreException;

    // end of splitting compaction
    void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC,
                                                                  FileInfo leftFileInfo,
                                                                  FileInfo rightFileInfo) throws StateStoreException;
}

interface CompactionAfter {
    // end of standard compaction
    void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(
            List<FileInfo> fileInPartitionRecordsToBeDeleted,
            FileInfo newActiveFile) throws StateStoreException;

    // end of splitting compaction
    void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(List<FileInfo> fileInPartitionRecordsToBeDeleted,
                                                                       FileInfo leftFileInfo,
                                                                       FileInfo rightFileInfo) throws StateStoreException;
}

interface GarbageCollectionBefore {
    Iterator<FileInfo> getReadyForGCFiles() throws StateStoreException;

    void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException;
}

interface GarbageCollectionAfter {
    Iterator<String> getReadyForGCFiles() throws StateStoreException;

    Iterator<FileLifecycleInfo> getReadyForGCFileInfos() throws StateStoreException;

    void findFilesThatShouldHaveStatusOfGCPending() throws StateStoreException;

    void deleteFileLifecycleEntries(List<String> filenames) throws StateStoreException;
}

public interface BeforeFileInfoStore {
    List<FileInfo> getActiveFiles() throws StateStoreException;

    List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException; // compaction creation

    Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException;
}

public interface AfterFileInfoStore {
    void atomicallySplitFileInPartitionRecord(FileInfo fileInPartitionRecordToBeSplit,
                                              String leftChildPartitionId, String rightChildPartitionId) throws StateStoreException; // metadata split

    List<FileInfo> getFileInPartitionList() throws StateStoreException;

    List<FileLifecycleInfo> getFileLifecycleList() throws StateStoreException;

    List<FileLifecycleInfo> getActiveFileList() throws StateStoreException;

    List<FileInfo> getFileInPartitionInfosWithNoJobId() throws StateStoreException;

    Map<String, List<String>> getPartitionToFileInPartitionMap() throws StateStoreException;
}
```

## Changing modules

#### Athena

- SleeperMetadataHandler
- getPartitionToActiveFilesMap > getPartitionToFileInPartitionMap
    - This finds relevant files for each leaf partition, to satisfy a query
    - Not clear whether it'll cause problems if the file is not in the leaf partition
    - Why is it only loading leaf partitions anyway?
    - Not changed yet in stage 1 or 2

#### Bulk Import

- No changes needed
- Only use of FileInfoStore is to call addFiles

#### CDK

- Only configuration for state store
    - table names
    - table definitions
    - permission names

#### Clients

- File status report
    - Needs to completely change!
    - Needs to report partition files separately from lifecycle
    - Some changes in stage 2
        - Not finished yet
        - Using file in partition data instead of lifecycle data
- Partitions report
    - Reports some information about the files in each partition
    - Not changed yet in stage 1 or 2
- ReinitialiseTable
    - Needs to clear the right tables
- QueryClient
    - getPartitionToActiveFilesMap > getPartitionToFileInPartitionMap
    - Used to initialise QueryExecutor
    - Is this needed? QueryExecutor already has the state store
    - Stage 1 just changes how QueryExecutor is called, need to check QueryExecutor directly

#### Compaction job creation

- CreateJobs
    - Needs to completely change
    - Before it creates compaction jobs, it makes sure all files are split
    - Compaction Strategies
        - Depends on how records are set in FileInfo
        - How can we know how many records are in partitions higher up the tree?
            - Estimates? - Part of compaction strategy?
            - Review compaction strategies and decide how they should work estimating record counts
            - We order by record count because compacting large files is not as useful as compacting small files

#### Compaction job execution

- CompactSortedFiles.createInputIterators
    - createInputIterators()
        - Filters iterators based on range of partition (File in compaction job may not be in the partition of the
          compaction job)
    - compactSplitting()
        - can be removed

#### Status store

- FileStatus has been removed
