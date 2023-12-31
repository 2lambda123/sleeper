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

package sleeper.clients.deploy;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

public class DeployInstanceConfiguration {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;

    private DeployInstanceConfiguration(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DeployInstanceConfiguration fromTemplateDirectory(Path templatesDir) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties(
                loadProperties(templatesDir.resolve("instanceproperties.template")));
        instanceProperties.loadTags(loadProperties(templatesDir.resolve("tags.template")));
        Properties properties = loadProperties(templatesDir.resolve("tableproperties.template"));
        properties.setProperty(TableProperty.SCHEMA.getPropertyName(),
                Files.readString(templatesDir.resolve("schema.template")));
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        return builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties).build();
    }

    public static DeployInstanceConfiguration fromInstancePropertiesOrTemplatesDir(Path instancePropertiesPath, Path templatesDir) throws IOException {
        if (instancePropertiesPath == null) {
            return fromTemplateDirectory(templatesDir);
        }
        Path rootDir = instancePropertiesPath.getParent();
        if (rootDir == null) {
            throw new IllegalArgumentException("Could not find parent of instance properties file");
        }
        InstanceProperties instanceProperties = new InstanceProperties(
                loadProperties(instancePropertiesPath));
        if (Files.exists(rootDir.resolve("tags.properties"))) {
            instanceProperties.loadTags(loadProperties(rootDir.resolve("tags.properties")));
        } else {
            instanceProperties.loadTags(loadProperties(templatesDir.resolve("tags.template")));
        }
        Properties properties;
        if (Files.exists(rootDir.resolve("table.properties"))) {
            properties = loadProperties(rootDir.resolve("table.properties"));
        } else {
            properties = loadProperties(templatesDir.resolve("tableproperties.template"));
        }
        if (Files.exists(rootDir.resolve("schema.json"))) {
            properties.setProperty(TableProperty.SCHEMA.getPropertyName(),
                    Files.readString(rootDir.resolve("schema.json")));
        } else {
            properties.setProperty(TableProperty.SCHEMA.getPropertyName(),
                    Files.readString(templatesDir.resolve("schema.template")));
        }
        TableProperties tableProperties = new TableProperties(instanceProperties, properties);
        return builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties).build();
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeployInstanceConfiguration that = (DeployInstanceConfiguration) o;
        return Objects.equals(instanceProperties, that.instanceProperties) && Objects.equals(tableProperties, that.tableProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceProperties, tableProperties);
    }

    @Override
    public String toString() {
        return "DeployInstanceConfiguration{" +
                "instanceProperties=" + instanceProperties +
                ", tableProperties=" + tableProperties +
                '}';
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties;

        public Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public DeployInstanceConfiguration build() {
            return new DeployInstanceConfiguration(this);
        }
    }
}
