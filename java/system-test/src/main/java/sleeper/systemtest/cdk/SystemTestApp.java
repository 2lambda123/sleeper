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
package sleeper.systemtest.cdk;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import sleeper.cdk.ConfigValidator;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.systemtest.SystemTestProperties;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;

import java.io.File;
import java.io.FileNotFoundException;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;

/**
 * An {@link App} to deploy the additional stacks needed for system tests.
 */
public class SystemTestApp extends Stack {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final SystemTestProperties testProperties;

    public SystemTestApp(
            App app, String id, StackProps props,
            InstanceProperties instanceProperties,
            TableProperties tableProperties,
            SystemTestProperties testProperties) {
        super(app, id, props);
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.testProperties = testProperties;
    }

    public void create() {

        // Stack for writing random data
        SystemTestStack systemTestStack = new SystemTestStack(this,
                "SystemTest", instanceProperties, tableProperties, testProperties);
        instanceProperties.getTags()
                .forEach((key, value) -> Tags.of(systemTestStack).add(key, value));
    }

    public static void main(String[] args) throws FileNotFoundException {
        App app = new App();

        String instancePropertiesFile = (String) app.getNode().tryGetContext("instancepropertiesfile");
        String tablePropertiesFile = (String) app.getNode().tryGetContext("tablepropertiesfile");
        String systemTestPropertiesFile = (String) app.getNode().tryGetContext("testpropertiesfile");
        String validate = (String) app.getNode().tryGetContext("validate");
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.load(new File(instancePropertiesFile));
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.load(new File(tablePropertiesFile));
        SystemTestProperties testProperties = new SystemTestProperties();
        testProperties.load(new File(systemTestPropertiesFile));
        if ("true".equalsIgnoreCase(validate)) {
            new ConfigValidator(AmazonS3ClientBuilder.defaultClient(),
                    AmazonDynamoDBClientBuilder.defaultClient()).validate(instanceProperties);
        }

        String id = instanceProperties.get(ID);
        Environment environment = Environment.builder()
                .account(instanceProperties.get(ACCOUNT))
                .region(instanceProperties.get(REGION))
                .build();

        new SystemTestApp(app, id, StackProps.builder()
                .stackName(id)
                .env(environment)
                .build(), instanceProperties, tableProperties, testProperties).create();
        app.synth();
    }
}
