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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.ContainerInstance;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesRequest;
import com.amazonaws.services.ecs.model.DescribeContainerInstancesResult;
import com.amazonaws.services.ecs.model.ListContainerInstancesRequest;
import com.amazonaws.services.ecs.model.ListContainerInstancesResult;
import com.amazonaws.services.ecs.model.Resource;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Details about EC2 instances in an ECS cluster.
 */
public class InstanceDetails {
    /** The container instance ARN. */
    public final String instanceArn;
    /** When was the instance registered with the cluster. */
    public final Instant registered;
    /** Amount of RAM available for container use. */
    public final int availableCPU;
    /** Amount of CPU available for container use. */
    public final int availableRAM;
    /** Amount of CPU in total. */
    public final int totalCPU;
    /** Amount of RAM in total. */
    public final int totalRAM;

    public InstanceDetails(String instanceArn, Instant registered, int availableCPU, int availableRAM, int totalCPU,
            int totalRAM) {
        super();
        this.instanceArn = instanceArn;
        this.registered = registered;
        this.availableCPU = availableCPU;
        this.availableRAM = availableRAM;
        this.totalCPU = totalCPU;
        this.totalRAM = totalRAM;
    }

    @Override
    public int hashCode() {
        return Objects.hash(availableCPU, availableRAM, instanceArn, registered, totalCPU, totalRAM);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        InstanceDetails other = (InstanceDetails) obj;
        return availableCPU == other.availableCPU && availableRAM == other.availableRAM
                && Objects.equals(instanceArn, other.instanceArn) && Objects.equals(registered, other.registered)
                && totalCPU == other.totalCPU && totalRAM == other.totalRAM;
    }

    @Override
    public String toString() {
        return "InstanceDetails [instance_arn=" + instanceArn + ", registered=" + registered + ", availableCPU="
                + availableCPU + ", availableRAM=" + availableRAM + ", totalCPU=" + totalCPU + ", totalRAM=" + totalRAM
                + "]";
    }

    /**
     * Find details of EC2 instances in an ECS cluster.
     * Inspects the cluster to find the details of all the instances.
     *
     * @param ecsClusterName the cluster name
     * @param ecsClient      the client conneciton
     * @return map of instance IDs to details
     */
    public static Map<String, InstanceDetails> fetchInstanceDetails(String ecsClusterName, AmazonECS ecsClient) {
        Map<String, InstanceDetails> details = new HashMap<>();
        // Loop over the container instances in page size of 100
        boolean more = true;
        ListContainerInstancesRequest req = new ListContainerInstancesRequest()
                .withCluster(ecsClusterName)
                .withMaxResults(100)
                .withStatus("ACTIVE");
        while (more) {
            ListContainerInstancesResult result = ecsClient.listContainerInstances(req);
            // More to come?
            more = result.getNextToken() != null;
            req = req.withNextToken(result.getNextToken());
            // check to see if there are any at all
            if (result.getContainerInstanceArns().isEmpty()) {
                continue;
            }
            // now get a description of these instances
            DescribeContainerInstancesRequest conReq = new DescribeContainerInstancesRequest()
                    .withCluster(ecsClusterName)
                    .withContainerInstances(result.getContainerInstanceArns());
            DescribeContainerInstancesResult containersResult = ecsClient.describeContainerInstances(conReq);
            for (ContainerInstance c : containersResult.getContainerInstances()) {
                // find the cpu and memory requirements
                List<Resource> totalResources = c.getRegisteredResources();
                List<Resource> remainingResources = c.getRemainingResources();
                details.put(c.getEc2InstanceId(), new InstanceDetails(c.getContainerInstanceArn(),
                        c.getRegisteredAt().toInstant(),
                        findResourceAmount("CPU", remainingResources),
                        findResourceAmount("MEMORY", remainingResources),
                        findResourceAmount("CPU", totalResources),
                        findResourceAmount("MEMORY", totalResources)));
            }
        }
        return details;
    }

    /**
     * Find the amount of the given resource in the list of resources.
     * The list is inspected for the named resource and returned as an integer.
     *
     * @param name      the resource name to find
     * @param resources the list of resources
     * @return the amount, or 0 if not known
     * @throws IllegalStateException if the resource type is not INTEGER
     */
    private static int findResourceAmount(String name, List<Resource> resources) {
        for (Resource r : resources) {
            if (r.getName().equals(name)) {
                if (!r.getType().equals("INTEGER")) {
                    throw new java.lang.IllegalStateException(
                            "resource " + name + " has type " + r.getType() + " instead of INTEGER");
                }
                return r.getIntegerValue();
            }
        }
        return 0;
    }
}