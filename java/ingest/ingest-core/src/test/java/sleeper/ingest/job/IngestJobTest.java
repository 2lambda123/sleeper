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

package sleeper.ingest.job;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestJobTest {

    @Test
    public void shouldSetRandomIdIfIdNotSet() {
        // Given/When
        IngestJob ingestJob = IngestJob.builder()
                .tableName("test-table")
                .files(Collections.singletonList("test.parquet"))
                .build();

        // Then
        assertThat(ingestJob.getId()).isNotEmpty();
    }

    @Test
    public void shouldSetRandomIdIfIdSetToEmptyString() {
        // Given/When
        IngestJob ingestJob = IngestJob.builder()
                .tableName("test-table")
                .files(Collections.singletonList("test.parquet"))
                .id("")
                .build();

        // Then
        assertThat(ingestJob.getId()).isNotEmpty();
    }
}
