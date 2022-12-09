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

package sleeper.status.report.partitions;

import sleeper.core.partition.Partition;

import java.io.PrintStream;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StandardPartitionsStatusReporter implements PartitionsStatusReporter {
    private final PrintStream out;

    public StandardPartitionsStatusReporter(PrintStream out) {
        this.out = out;
    }

    public void report(PartitionsQuery query, List<Partition> partitions, Predicate<Partition> splittingCheck) {
        if (query == PartitionsQuery.ALL) {
            printAllPartitions(partitions, splittingCheck);
        } else {
            throw new IllegalArgumentException("Unrecognised query type: " + query);
        }
    }

    private void printAllPartitions(List<Partition> partitions, Predicate<Partition> splittingCheck) {
        out.println();
        out.println("Partitions Status Report:");
        out.println("--------------------------");
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        out.println("There are " + partitions.size() + " partitions (" + leafPartitions.size() + " leaf partitions)");
        List<Partition> partitionsThatNeedSplitting = leafPartitions.stream().filter(splittingCheck).collect(Collectors.toList());
        out.println("There are " + partitionsThatNeedSplitting.size() + " leaf partitions that need splitting");
        partitions.forEach(out::println);
        out.println("--------------------------");
    }
}
