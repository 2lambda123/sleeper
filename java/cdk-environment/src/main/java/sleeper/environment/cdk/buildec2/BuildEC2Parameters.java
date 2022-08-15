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
package sleeper.environment.cdk.buildec2;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.StringParameter;

import java.util.Objects;

public class BuildEC2Parameters {

    public static StringParameter REPOSITORY = AppParameters.BUILD_REPOSITORY;
    public static StringParameter FORK = AppParameters.BUILD_FORK;
    public static StringParameter BRANCH = AppParameters.BUILD_BRANCH;

    private final String repository;
    private final String fork;
    private final String branch;

    private BuildEC2Parameters(Builder builder) {
        repository = requireNonEmpty(builder.repository, "repository must not be empty");
        fork = requireNonEmpty(builder.fork, "fork must not be empty");
        branch = requireNonEmpty(builder.branch, "branch must not be empty");
    }

    public static BuildEC2Parameters from(AppContext context) {
        return builder()
                .repository(context.get(REPOSITORY))
                .fork(context.get(FORK))
                .branch(context.get(BRANCH))
                .build();
    }

    String fillUserDataTemplate(String template) {
        return template.replace("${repository}", repository)
                .replace("${fork}", fork)
                .replace("${branch}", branch);
    }

    private static String requireNonEmpty(String value, String message) {
        Objects.requireNonNull(value, message);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String repository;
        private String fork;
        private String branch;

        private Builder() {
        }

        public Builder repository(String repository) {
            this.repository = repository;
            return this;
        }

        public Builder fork(String fork) {
            this.fork = fork;
            return this;
        }

        public Builder branch(String branch) {
            this.branch = branch;
            return this;
        }

        public BuildEC2Parameters build() {
            return new BuildEC2Parameters(this);
        }
    }
}
