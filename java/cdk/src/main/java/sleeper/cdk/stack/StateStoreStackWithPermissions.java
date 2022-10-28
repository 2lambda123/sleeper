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

import software.amazon.awscdk.services.iam.IGrantable;

public interface StateStoreStackWithPermissions extends StateStoreStack {

    StateStoreStack getPermissions();

    default void grantReadActiveFileMetadata(IGrantable grantee) {
        getPermissions().grantReadActiveFileMetadata(grantee);
    }

    default void grantReadWriteActiveFileMetadata(IGrantable grantee) {
        getPermissions().grantReadWriteActiveFileMetadata(grantee);
    }

    default void grantReadWriteReadyForGCFileMetadata(IGrantable grantee) {
        getPermissions().grantReadWriteReadyForGCFileMetadata(grantee);
    }

    default void grantWriteReadyForGCFileMetadata(IGrantable grantee) {
        getPermissions().grantWriteReadyForGCFileMetadata(grantee);
    }

    default void grantReadPartitionMetadata(IGrantable grantee) {
        getPermissions().grantReadPartitionMetadata(grantee);
    }

    default void grantReadWritePartitionMetadata(IGrantable grantee) {
        getPermissions().grantReadWritePartitionMetadata(grantee);
    }

}
