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

package sleeper.build.maven;

import sleeper.build.chunks.ProjectStructure;
import sleeper.build.maven.dependencydraw.DrawDependencyGraph;

import java.io.IOException;
import java.nio.file.Path;

public class ShowInternalDependencies {
    private ShowInternalDependencies() {
    }

    public static void main(String[] args) throws IOException {
        Path repositoryRoot = Path.of("").toAbsolutePath();
        MavenModuleStructure mavenStructure = ProjectStructure.builder()
                .chunksYamlPath(repositoryRoot.resolve(".github/config/chunks.yaml"))
                .mavenProjectPath(repositoryRoot.resolve("java"))
                .workflowsPath(repositoryRoot.resolve(".github/config/workflows"))
                .build().loadMavenStructure();
        DrawDependencyGraph drawDependencyGraph = new DrawDependencyGraph();
        drawDependencyGraph.produceGraphFromMaven(mavenStructure);
    }
}
