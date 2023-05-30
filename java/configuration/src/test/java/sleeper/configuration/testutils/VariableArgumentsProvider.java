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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class VariableArgumentsProvider implements ArgumentsProvider, AnnotationConsumer<VariableSource> {

    private String variableName;

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return context.getTestClass()
                .map(this::getTopLevelClass)
                .map(this::getField)
                .map(field -> getValue(field, context))
                .orElseThrow(() ->
                        new IllegalArgumentException("Failed to load test arguments"));
    }

    @Override
    public void accept(VariableSource variableSource) {
        variableName = variableSource.value();
    }

    private Class<?> getTopLevelClass(Class<?> clazz) {
        return Objects.requireNonNullElse(clazz.getEnclosingClass(), clazz);
    }

    private Field getField(Class<?> clazz) {
        try {
            return clazz.getDeclaredField(variableName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<? extends Arguments> getValue(Field field, ExtensionContext context) {
        Object value;
        try {
            value = field.get(null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        if (value instanceof List) {
            return ((List<? extends Arguments>) value).stream();
        } else if (value instanceof ArgumentsProvider) {
            try {
                return ((ArgumentsProvider) value).provideArguments(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException("Unsupported value for variable " + variableName + ": " + value);
        }
    }
}
