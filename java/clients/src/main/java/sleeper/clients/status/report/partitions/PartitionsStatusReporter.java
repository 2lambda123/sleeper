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

package sleeper.clients.status.report.partitions;

import sleeper.clients.status.report.job.StandardProcessRunReporter;
import sleeper.clients.util.table.TableField;
import sleeper.clients.util.table.TableRow;
import sleeper.clients.util.table.TableWriterFactory;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Field;

import java.io.PrintStream;

public class PartitionsStatusReporter {

    private static final int SPLIT_VALUE_MAX_LENGTH = 30;

    private static final TableWriterFactory.Builder BUILDER = TableWriterFactory.builder();
    private static final TableField ID = BUILDER.addField("ID");
    private static final TableField PARENT = BUILDER.addField("PARENT");
    private static final TableField PARENT_SIDE = BUILDER.addField("PARENT_SIDE");
    private static final TableField FILES = BUILDER.addNumericField("FILES");
    private static final TableField RECORDS = BUILDER.addNumericField("RECORDS");
    private static final TableField LEAF = BUILDER.addField("LEAF");
    private static final TableField NEEDS_SPLITTING = BUILDER.addField("NEEDS_SPLITTING");
    private static final TableField SPLIT_FIELD = BUILDER.addField("SPLIT_FIELD");
    private static final TableField SPLIT_VALUE = BUILDER.addField("SPLIT_VALUE");
    private static final TableWriterFactory TABLE_FACTORY = BUILDER.build();

    private final PrintStream out;

    public PartitionsStatusReporter(PrintStream out) {
        this.out = out;
    }

    public void report(PartitionsStatus status) {
        out.println();
        out.println("Partitions Status Report:");
        out.println("--------------------------");
        out.println("There are " + status.getNumPartitions() + " partitions (" + status.getNumLeafPartitions() + " leaf partitions)");
        out.println("There are " + status.getNumSplittingPartitions() + " leaf partitions that need splitting");
        out.println("Split threshold is " + status.getSplitThreshold() + " records");
        TABLE_FACTORY.tableBuilder()
                .itemsAndWriter(status.getPartitions(), PartitionsStatusReporter::writeRow)
                .build().write(out);
    }

    private static void writeRow(PartitionStatus status, TableRow.Builder builder) {
        Partition partition = status.getPartition();
        builder.value(ID, partition.getId())
                .value(PARENT, partition.getParentPartitionId())
                .value(PARENT_SIDE, parentSideString(status))
                .value(FILES, status.getNumberOfFiles())
                .value(RECORDS, status.getNumberOfRecords())
                .value(LEAF, partition.isLeafPartition() ? "yes" : "no")
                .value(NEEDS_SPLITTING, needsSplittingString(status))
                .value(SPLIT_FIELD, StandardProcessRunReporter.getOrNull(status.getSplitField(), Field::getName))
                .value(SPLIT_VALUE, splitValueString(status));
    }

    private static String needsSplittingString(PartitionStatus status) {
        if (!status.isLeafPartition()) {
            return null;
        }
        return status.isNeedsSplitting() ? "yes" : "no";
    }

    private static String splitValueString(PartitionStatus status) {
        Object value = status.getSplitValue();
        if (value instanceof byte[]) {
            return "[raw bytes]";
        }
        if (value == null) {
            return null;
        }
        String string = value.toString();
        if (string.length() < SPLIT_VALUE_MAX_LENGTH) {
            return string;
        }
        int partSize = SPLIT_VALUE_MAX_LENGTH / 2;
        return string.substring(0, partSize) + "..." + string.substring(string.length() - partSize);
    }

    private static String parentSideString(PartitionStatus status) {
        Integer index = status.getIndexInParent();
        if (index == null) {
            return null;
        } else if (index == 0) {
            return "min";
        } else if (index == 1) {
            return "max";
        } else {
            return "" + index;
        }
    }
}
