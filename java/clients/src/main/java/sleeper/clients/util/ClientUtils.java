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
package sleeper.clients.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ClientUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtils.class);

    private ClientUtils() {
    }

    public static InstanceProperties getInstanceProperties(String instanceId) throws IOException {
        return getInstanceProperties(AmazonS3ClientBuilder.defaultClient(), instanceId);
    }

    public static InstanceProperties getInstanceProperties(AmazonS3 amazonS3, String instanceId) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(amazonS3, instanceId);
        return instanceProperties;
    }

    public static Optional<String> optionalArgument(String[] args, int index) {
        if (args.length > index) {
            return Optional.of(args[index]);
        } else {
            return Optional.empty();
        }
    }

    private static final long K_COUNT = 1_000;
    private static final long M_COUNT = 1_000_000;
    private static final long G_COUNT = 1_000_000_000;
    private static final long T_COUNT = 1_000_000_000_000L;

    public static String abbreviatedRecordCount(long records) {
        if (records < K_COUNT) {
            return "" + records;
        } else if (records < M_COUNT) {
            return Math.round((double) records / K_COUNT) + "K (" + countWithCommas(records) + ")";
        } else if (records < G_COUNT) {
            return Math.round((double) records / M_COUNT) + "M (" + countWithCommas(records) + ")";
        } else if (records < T_COUNT) {
            return Math.round((double) records / G_COUNT) + "G (" + countWithCommas(records) + ")";
        } else {
            return countWithCommas(Math.round((double) records / T_COUNT)) + "T (" + countWithCommas(records) + ")";
        }
    }

    public static String countWithCommas(long count) {
        return splitNonDecimalIntoParts("" + count);
    }

    public static String decimalWithCommas(String formatStr, double decimal) {
        String str = String.format(formatStr, decimal);
        int decimalIndex = str.indexOf('.');
        if (decimalIndex > 0) {
            return splitNonDecimalIntoParts(str.substring(0, decimalIndex)) + str.substring(decimalIndex);
        } else {
            return splitNonDecimalIntoParts(str);
        }
    }

    private static String splitNonDecimalIntoParts(String str) {
        int length = str.length();
        int firstPartEnd = length % 3;

        List<String> parts = new ArrayList<>();
        if (firstPartEnd != 0) {
            parts.add(str.substring(0, firstPartEnd));
        }
        for (int i = firstPartEnd; i < length; i += 3) {
            parts.add(str.substring(i, i + 3));
        }
        return String.join(",", parts);
    }

    public static void clearDirectory(Path tempDir) throws IOException {
        try (Stream<Path> paths = Files.walk(tempDir)) {
            Stream<Path> nestedPaths = paths.skip(1).sorted(Comparator.reverseOrder());
            for (Path path : (Iterable<Path>) nestedPaths::iterator) {
                Files.delete(path);
            }
        }
    }

    public static int runCommandLogOutput(String... commands) throws IOException, InterruptedException {
        LOGGER.info("Running command: {}", (Object) commands);
        Process process = new ProcessBuilder(commands).start();
        CompletableFuture<Void> logOutput = CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> logTo(process.getInputStream(), LOGGER::info)),
                CompletableFuture.runAsync(() -> logTo(process.getErrorStream(), LOGGER::error)));
        int exitCode = process.waitFor();
        logOutput.join();
        LOGGER.info("Exit code: {}", exitCode);
        return exitCode;
    }

    private static void logTo(InputStream stream, Consumer<String> logLine) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            String line = reader.readLine();
            while (line != null) {
                logLine.accept(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static int runCommandInheritIO(String... commands) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(commands).inheritIO().start();
        return process.waitFor();
    }
}
