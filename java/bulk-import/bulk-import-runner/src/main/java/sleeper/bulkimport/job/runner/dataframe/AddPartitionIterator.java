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
package sleeper.bulkimport.job.runner.dataframe;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An {@link Iterator} of {@link Row}s that takes an existing {@link Iterator}
 * of {@link Row}s and adds the id of the partition the key from each {@link Row}.
 */
public class AddPartitionIterator implements Iterator<Row> {
    private final Iterator<Row> input;
    private final PartitionTree partitionTree;
    private final int numRowKeyFields;
    private final int numFields;

    public AddPartitionIterator(Iterator<Row> input, Schema schema, PartitionTree partitionTree) {
        this.input = input;
        this.partitionTree = partitionTree;
        this.numRowKeyFields = schema.getRowKeyFieldNames().size();
        this.numFields = schema.getAllFieldNames().size();
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public Row next() {
        Row row = input.next();

        Object[] rowWithPartition = new Object[numFields + 1];
        List<Object> key = new ArrayList<>(numRowKeyFields);
        for (int i = 0; i < numFields; i++) {
            rowWithPartition[i] = row.get(i);
            if (i < numRowKeyFields) {
                key.add(rowWithPartition[i]);
            }
        }

        String partitionId = partitionTree.getLeafPartition(Key.create(key)).getId();
        rowWithPartition[numFields] = partitionId;

        return RowFactory.create(rowWithPartition);
    }
}
