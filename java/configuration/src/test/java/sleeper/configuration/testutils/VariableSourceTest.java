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
package sleeper.configuration.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class VariableSourceTest {

    private static final ArgumentsProvider PROVIDER = context -> Stream.of(
            Arguments.of("arg-1", "arg-2")
    );

    private static final List<Arguments> LIST = List.of(
            Arguments.of("arg-1", "arg-2")
    );

    @Nested
    @DisplayName("Read a variable from a nested class")
    class ReadVariableInNestedClass {

        @ParameterizedTest
        @VariableSource("PROVIDER")
        void shouldReadArgumentsProviderVariableFromOuterClass(String arg1, String arg2) {
            assertThat(List.of(arg1, arg2)).containsExactly("arg-1", "arg-2");
        }

        @ParameterizedTest
        @VariableSource("LIST")
        void shouldReadArgumentsListVariableFromOuterClass(String arg1, String arg2) {
            assertThat(List.of(arg1, arg2)).containsExactly("arg-1", "arg-2");
        }
    }

    @ParameterizedTest
    @VariableSource("PROVIDER")
    void shouldReadVariableWhenInOuterClass(String arg1, String arg2) {
        assertThat(List.of(arg1, arg2)).containsExactly("arg-1", "arg-2");
    }
}
