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
package sleeper.cdk.stack;

import software.amazon.awscdk.services.dynamodb.ITable;
import software.amazon.awscdk.services.iam.IGrantable;

public class DynamoDBStateStorePermissions implements StateStoreStack {

    private final ITable activeFileInfoTable;
    private final ITable readyForGCFileInfoTable;
    private final ITable partitionTable;

    private DynamoDBStateStorePermissions(Builder builder) {
        activeFileInfoTable = builder.activeFileInfoTable;
        readyForGCFileInfoTable = builder.readyForGCFileInfoTable;
        partitionTable = builder.partitionTable;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void grantReadActiveFileMetadata(IGrantable grantee) {
        activeFileInfoTable.grantReadData(grantee);
    }

    @Override
    public void grantReadWriteActiveFileMetadata(IGrantable grantee) {
        activeFileInfoTable.grantReadWriteData(grantee);
    }

    @Override
    public void grantReadWriteReadyForGCFileMetadata(IGrantable grantee) {
        readyForGCFileInfoTable.grantReadWriteData(grantee);
    }

    @Override
    public void grantWriteReadyForGCFileMetadata(IGrantable grantee) {
        readyForGCFileInfoTable.grantWriteData(grantee);
    }

    @Override
    public void grantReadPartitionMetadata(IGrantable grantee) {
        partitionTable.grantReadData(grantee);
    }

    @Override
    public void grantReadWritePartitionMetadata(IGrantable grantee) {
        partitionTable.grantReadWriteData(grantee);
    }

    public static final class Builder {
        private ITable activeFileInfoTable;
        private ITable readyForGCFileInfoTable;
        private ITable partitionTable;

        private Builder() {
        }

        public Builder activeFileInfoTable(ITable activeFileInfoTable) {
            this.activeFileInfoTable = activeFileInfoTable;
            return this;
        }

        public Builder readyForGCFileInfoTable(ITable readyForGCFileInfoTable) {
            this.readyForGCFileInfoTable = readyForGCFileInfoTable;
            return this;
        }

        public Builder partitionTable(ITable partitionTable) {
            this.partitionTable = partitionTable;
            return this;
        }

        public DynamoDBStateStorePermissions build() {
            return new DynamoDBStateStorePermissions(this);
        }
    }
}
