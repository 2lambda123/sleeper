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
package sleeper.cdk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import static org.assertj.core.api.Assertions.assertThat;

class BuiltJarsTest {

    @TempDir
    private Path tempDir;

    @Test
    void shouldPopulateFilenameWithVersion() {
        BuiltJars jars = BuiltJars.withVersionAndPath("1.0", tempDir);
        assertThat(jars.jar(new BuiltJars.Jar("test-%s.jar")).fileName())
                .isEqualTo("test-1.0.jar");
    }

    @Test
    void shouldComputeShaForAFile() throws IOException, NoSuchAlgorithmException {
        BuiltJars jars = BuiltJars.withVersionAndPath("1.0", tempDir);
        Files.writeString(tempDir.resolve("test-1.0.jar"), "foobar");

        assertThat(jars.jar(new BuiltJars.Jar("test-%s.jar")).codeSha256())
                .isEqualTo("w6uP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=");
    }
}
