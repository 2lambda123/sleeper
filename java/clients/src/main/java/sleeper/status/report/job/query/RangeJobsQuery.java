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
package sleeper.status.report.job.query;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.console.ConsoleInput;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Supplier;

public class RangeJobsQuery implements JobQuery {

    public static final String DATE_FORMAT = "yyyyMMddHHmmss";

    private final String tableName;
    private final Instant start;
    private final Instant end;

    public RangeJobsQuery(String tableName, Instant start, Instant end) {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Start of range provided is after end");
        }
        this.tableName = tableName;
        this.start = start;
        this.end = end;
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobStatusStore statusStore) {
        return statusStore.getJobsInTimePeriod(tableName, start, end);
    }

    @Override
    public List<IngestJobStatus> run(IngestJobStatusStore statusStore) {
        return statusStore.getJobsInTimePeriod(tableName, start, end);
    }

    public static JobQuery fromParameters(String tableName, String queryParameters, Clock clock) {
        if (queryParameters == null) {
            Instant end = clock.instant();
            Instant start = end.minus(Duration.ofHours(4));
            return new RangeJobsQuery(tableName, start, end);
        } else {
            String[] parts = queryParameters.split(",");
            Instant start = parseStart(parts[0], clock);
            Instant end = parseEnd(parts[1], clock);
            return new RangeJobsQuery(tableName, start, end);
        }
    }

    public static JobQuery prompt(String tableName, ConsoleInput in, Clock clock) {
        Instant start = promptStart(in, clock);
        Instant end = promptEnd(in, clock);
        return new RangeJobsQuery(tableName, start, end);
    }

    private static Instant promptStart(ConsoleInput in, Clock clock) {
        String str = in.promptLine("Enter range start in format " + DATE_FORMAT + " (default is 4 hours ago): ");
        try {
            return parseStart(str, clock);
        } catch (IllegalArgumentException e) {
            return promptStart(in, clock);
        }
    }

    private static Instant promptEnd(ConsoleInput in, Clock clock) {
        String str = in.promptLine("Enter range end in format " + DATE_FORMAT + " (default is now): ");
        try {
            return parseEnd(str, clock);
        } catch (IllegalArgumentException e) {
            return promptEnd(in, clock);
        }
    }

    private static Instant parseStart(String startStr, Clock clock) {
        return parseDate(startStr, () -> clock.instant().minus(Duration.ofHours(4)));
    }

    private static Instant parseEnd(String endStr, Clock clock) {
        return parseDate(endStr, clock::instant);
    }

    private static Instant parseDate(String input, Supplier<Instant> getDefault) {
        if ("".equals(input)) {
            return getDefault.get();
        }
        try {
            return createDateInputFormat().parse(input).toInstant();
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static SimpleDateFormat createDateInputFormat() {
        SimpleDateFormat dateInputFormat = new SimpleDateFormat(DATE_FORMAT);
        dateInputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateInputFormat;
    }
}