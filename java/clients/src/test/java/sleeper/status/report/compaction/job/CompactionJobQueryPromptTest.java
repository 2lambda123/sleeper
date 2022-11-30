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

package sleeper.status.report.compaction.job;

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static sleeper.status.report.compaction.job.CompactionJobStatusReporter.QueryType;

public class CompactionJobQueryPromptTest extends CompactionJobQueryTestBase {
    @Test
    public void shouldCreateAllQueryWithNoParameters() {
        // Given
        when(statusStore.getAllJobs(tableName)).thenReturn(exampleStatusList);
        consoleInput.enterNextPrompt("a");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(printStream).hasToString("All (a), Detailed (d), range (r), or unfinished (u) query? \n");
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateUnfinishedQueryWithNoParameters() {
        // Given
        when(statusStore.getUnfinishedJobs(tableName)).thenReturn(exampleStatusList);
        consoleInput.enterNextPrompt("u");

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(printStream).hasToString("All (a), Detailed (d), range (r), or unfinished (u) query? \n");
        assertThat(statuses).isEqualTo(exampleStatusList);
    }

    @Test
    public void shouldCreateDetailedQueryWithSpecifiedJobIds() {
        // Given
        String queryParameters = "job1,job2";
        when(statusStore.getJob("job1")).thenReturn(Optional.of(exampleStatus1));
        when(statusStore.getJob("job2")).thenReturn(Optional.of(exampleStatus2));
        consoleInput.enterNextPrompts("d", queryParameters);

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(printStream).hasToString("" +
                "All (a), Detailed (d), range (r), or unfinished (u) query? \n" +
                "Enter jobId to get detailed information about: \n");
        assertThat(statuses).containsExactly(exampleStatus1, exampleStatus2);
    }

    @Test
    public void shouldCreateDetailedQueryWithNoJobIds() {
        // Given
        String queryParameters = "";
        consoleInput.enterNextPrompts("d", queryParameters);

        // When
        List<CompactionJobStatus> statuses = queryStatusByPrompt();

        // Then
        assertThat(printStream).hasToString("" +
                "All (a), Detailed (d), range (r), or unfinished (u) query? \n" +
                "Enter jobId to get detailed information about: \n");
        assertThat(statuses).isEmpty();
    }

    private List<CompactionJobStatus> queryStatusByPrompt() {
        return queryStatuses(QueryType.PROMPT);
    }
}
