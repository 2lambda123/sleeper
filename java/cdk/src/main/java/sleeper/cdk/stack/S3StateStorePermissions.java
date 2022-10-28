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
import software.amazon.awscdk.services.s3.IBucket;

public class S3StateStorePermissions implements StateStoreStack {

    private final ITable revisionTable;
    private final IBucket dataBucket;

    private S3StateStorePermissions(Builder builder) {
        revisionTable = builder.revisionTable;
        dataBucket = builder.dataBucket;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void grantReadActiveFileMetadata(IGrantable grantee) {
        grantRead(grantee);
    }

    @Override
    public void grantReadWriteActiveFileMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    @Override
    public void grantReadWriteReadyForGCFileMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    @Override
    public void grantWriteReadyForGCFileMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    @Override
    public void grantReadPartitionMetadata(IGrantable grantee) {
        grantRead(grantee);
    }

    @Override
    public void grantReadWritePartitionMetadata(IGrantable grantee) {
        grantReadWrite(grantee);
    }

    private void grantReadWrite(IGrantable grantee) {
        revisionTable.grantReadWriteData(grantee);
        dataBucket.grantReadWrite(grantee); // TODO Only needs access to keys starting with 'statestore'
    }

    private void grantRead(IGrantable grantee) {
        revisionTable.grantReadData(grantee);
        dataBucket.grantRead(grantee); // TODO Only needs access to keys starting with 'statestore'
    }

    public static final class Builder {
        private ITable revisionTable;
        private IBucket dataBucket;

        private Builder() {
        }

        public Builder revisionTable(ITable revisionTable) {
            this.revisionTable = revisionTable;
            return this;
        }

        public Builder dataBucket(IBucket dataBucket) {
            this.dataBucket = dataBucket;
            return this;
        }

        public S3StateStorePermissions build() {
            return new S3StateStorePermissions(this);
        }
    }
}
