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
package sleeper.build.github;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubCompareResponse {

    private final int aheadBy;
    private final int behindBy;
    private final List<File> files;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GitHubCompareResponse(
            @JsonProperty("ahead_by") int aheadBy,
            @JsonProperty("behind_by") int behindBy,
            @JsonProperty("files") List<File> files) {
        this.aheadBy = aheadBy;
        this.behindBy = behindBy;
        this.files = files == null ? Collections.emptyList() : files;
    }

    public GitHubRunToHead toRunToHead(GitHubWorkflowRun run) {
        return new GitHubRunToHead(run, behindBy < 1);
    }

    public static class File {
        private final String filename;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public File(
                @JsonProperty("filename") String filename) {
            this.filename = filename;
        }
    }
}