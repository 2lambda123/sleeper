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
package sleeper.status.report.compactionjob;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;

public class CompactionJobStatusReportArgumentsTest {

    @Test
    public void shouldSetDefaultArguments() {
        assertThat(CompactionJobStatusReportArguments.from("test-instance", "test-table"))
                .usingRecursiveComparison()
                .isEqualTo(CompactionJobStatusReportArguments.builder()
                        .instanceId("test-instance").tableName("test-table")
                        .queryType(QueryType.PROMPT)
                        .reporter(new StandardCompactionJobStatusReporter(System.out))
                        .build());
    }

    @Test
    public void shouldFailWhenNoParametersSetForRangeQuery() {
        assertThatThrownBy(() ->
                CompactionJobStatusReportArguments.from("test-instance", "test-table", "standard", "-r"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No parameters provided for query type RANGE");
    }
}