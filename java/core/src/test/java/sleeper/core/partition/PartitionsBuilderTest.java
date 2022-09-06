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
package sleeper.core.partition;

import org.junit.Test;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PartitionsBuilderTest {

    @Test
    public void canBuildPartitionsSpecifyingSplitPointsLeavesFirst() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C"),
                        Arrays.asList("aaa", "bbb"))
                .parentJoining("D", "A", "B")
                .parentJoining("E", "D", "C");

        // Then
        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        List<Partition> expectedPartitions = Arrays.asList(
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(field, "", "aaa")),
                        "A", true, "D", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(field, "aaa", "bbb")),
                        "B", true, "D", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(field, "bbb", null)),
                        "C", true, "E", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(field, "", "bbb")),
                        "D", false, "E", Arrays.asList("A", "B"), 0),
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(field, "", null)),
                        "E", false, null, Arrays.asList("D", "C"), 0));
        assertThat(builder.buildList()).isEqualTo(expectedPartitions);
        assertThat(builder.buildTree()).isEqualTo(new PartitionTree(schema, expectedPartitions));
    }

    @Test
    public void canBuildPartitionsSpecifyingSplitPointsLeavesFirstWhenOnlyCareAboutLeaves() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When I only care about leaf partitions, so I want any tree without caring about
        // the structure or the non-leaf IDs
        PartitionTree tree = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C"),
                        Arrays.asList("aaa", "bbb"))
                .anyTreeJoiningAllLeaves()
                .buildTree();

        // Then all leaves have a path to the root partition
        assertThat(tree.getAllAncestors("A")).endsWith(tree.getRootPartition());
        assertThat(tree.getAllAncestors("B")).endsWith(tree.getRootPartition());
        assertThat(tree.getAllAncestors("C")).endsWith(tree.getRootPartition());
    }

    @Test
    public void failJoiningAllLeavesIfNonLeafSpecified() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C"),
                        Arrays.asList("aaa", "bbb"))
                .parentJoining("D", "A", "B");

        // When / Then
        assertThatThrownBy(builder::anyTreeJoiningAllLeaves)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
