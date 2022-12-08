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

package sleeper.status.report.ingest.job;

import sleeper.console.ConsoleInput;
import sleeper.status.report.job.query.JobQuery;
import sleeper.status.report.job.query.JobQueryArgument;

import java.io.PrintStream;
import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.ClientUtils.optionalArgument;

public class IngestJobStatusReportArguments {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, IngestJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardIngestJobStatusReporter());
        REPORTERS.put("JSON", new JsonIngestJobStatusReporter());
    }

    private final String instanceId;
    private final String tableName;
    private final IngestJobStatusReporter reporter;
    private final JobQuery.Type queryType;
    private final String queryParameters;

    private IngestJobStatusReportArguments(Builder builder) {
        instanceId = builder.instanceId;
        tableName = builder.tableName;
        reporter = builder.reporter;
        queryType = builder.queryType;
        queryParameters = builder.queryParameters;
    }

    public static void printUsage(PrintStream out) {
        out.println("Usage: <instance id> <table name> <report_type_standard_or_json> <optional_query_type> <optional_query_parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-u (Unfinished jobs)");
    }

    public static IngestJobStatusReportArguments from(String... args) {
        if (args.length < 2 || args.length > 5) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        return builder()
                .instanceId(args[0])
                .tableName(args[1])
                .reporter(getReporter(args, 2))
                .queryType(JobQueryArgument.readTypeArgument(args, 3))
                .queryParameters(optionalArgument(args, 4).orElse(null))
                .build();
    }

    public JobQuery buildQuery(Clock clock, ConsoleInput input) {
        return JobQuery.fromParametersOrPrompt(tableName, queryType, queryParameters, clock, input);
    }

    private static IngestJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getTableName() {
        return tableName;
    }

    public IngestJobStatusReporter getReporter() {
        return reporter;
    }

    public JobQuery.Type getQueryType() {
        return queryType;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String instanceId;
        private String tableName;
        private IngestJobStatusReporter reporter;
        private JobQuery.Type queryType;
        private String queryParameters;

        private Builder() {
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder reporter(IngestJobStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public Builder queryType(JobQuery.Type queryType) {
            this.queryType = queryType;
            return this;
        }

        public Builder queryParameters(String queryParameters) {
            this.queryParameters = queryParameters;
            return this;
        }

        public IngestJobStatusReportArguments build() {
            return new IngestJobStatusReportArguments(this);
        }
    }
}