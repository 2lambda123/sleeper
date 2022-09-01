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

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A convenience class for specifying partitions.
 * <p>
 * Note that a shorthand is used for cases where we have a schema with only one row key field.
 * This will not be useful in the general case.
 */
public class PartitionsBuilder {

    private final Schema schema;
    private final RangeFactory rangeFactory;
    private final List<PrimitiveType> rowKeyTypes;
    private final List<Partition> partitions = new ArrayList<>();
    private final Map<String, Partition> partitionById = new HashMap<>();

    public PartitionsBuilder(Schema schema) {
        this.schema = schema;
        rangeFactory = new RangeFactory(schema);
        rowKeyTypes = schema.getRowKeyTypes();
    }

    public PartitionsBuilder root(String id, Object min, Object max) {
        partition(id, min, max);
        return this;
    }

    public PartitionsBuilder split(String parentId, String leftId, String rightId, Object splitPoint) {
        Partition parent = partitionById(parentId);
        Range parentRange = singleRange(parent);
        child(parent, leftId, parentRange.getMin(), splitPoint);
        child(parent, rightId, splitPoint, parentRange.getMax());
        return this;
    }

    public Partition partition(String id, Object min, Object max) {
        Partition partition = new Partition(rowKeyTypes,
                new Region(rangeFactory.createRange(singleRowKeyField(), min, max)),
                id, true, null, Collections.emptyList(), -1);
        partitions.add(partition);
        partitionById.put(id, partition);
        return partition;
    }

    public Partition child(Partition parent, String id, Object min, Object max) {
        Partition child = partition(id, min, max);
        child.setParentPartitionId(parent.getId());
        parent.getChildPartitionIds().add(id);
        parent.setLeafPartition(false);
        parent.setDimension(0);
        return child;
    }

    public Partition parent(List<Partition> children, String id, Object min, Object max) {
        Partition parent = partition(id, min, max);
        parent.setChildPartitionIds(children.stream()
                .map(Partition::getId)
                .collect(Collectors.toList()));
        parent.setLeafPartition(false);
        parent.setDimension(0);
        children.forEach(child -> child.setParentPartitionId(id));
        return parent;
    }

    private Partition partitionById(String id) {
        return Optional.ofNullable(partitionById.get(id))
                .orElseThrow(() -> new IllegalArgumentException("Partition not specified: " + id));
    }

    private Range singleRange(Partition partition) {
        return partition.getRegion().getRange(singleRowKeyField().getName());
    }

    public List<Partition> buildList() {
        return new ArrayList<>(partitions);
    }

    public PartitionTree buildTree() {
        return new PartitionTree(schema, partitions);
    }

    private Field singleRowKeyField() {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        if (rowKeyFields.size() != 1) {
            throw new IllegalStateException("Cannot get single row key field, have " + rowKeyFields.size() + " row key fields");
        }
        return rowKeyFields.get(0);
    }
}
