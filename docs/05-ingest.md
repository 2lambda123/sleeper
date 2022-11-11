Ingesting data
==============

## Introduction

Data in Sleeper tables is stored partitioned by the key and sorted within those partitions. Therefore when
Sleeper is given some data to ingest it must partition and sort it. This data must then be written to Parquet files
(one per leaf partition) and then the state store must be updated so that it is aware that the new data is in the
table.

There are two ways of ingesting data: standard ingest and bulk import. The former refers to a process that runs in
a container that reads data and partitions and sorts it locally before writing it to files in S3. Scalability is
achieved by running many of these in parallel. Bulk import means using [Apache Spark](https://spark.apache.org/)
to run a MapReduce-like job to partition and sort a batch of data so that it can be ingested into a Sleeper table.

The standard ingest process can be called from Java on any `Iterable` of `Record`s. There is also an `IngestStack` which
allows you to provide the data to be ingested as Parquet files. By sending a message to an SQS queue you can tell
Sleeper to ingest this data. Sleeper will spin up ECS tasks to perform this ingest.

Ingesting data using the bulk import approach requires the data to first be written to Parquet files. Then you
tell Sleeper to ingest that data by sending a message to an SQS queue. This will use an EMR cluster to run the
Spark job to perform the ingest. There are two stacks that can be used for this approach: the `EmrBulkImportStack` and
the `PersistentEmrBulkImportStack`. The former creates an EMR cluster on demand to run the Spark job. The cluster is
only used for that bulk import job. The latter creates an EMR cluster that is permanently running. By default it
scales up and down so that if there are no bulk import jobs to run then minimal resources will be used. There is
also an experimental option to run bulk import jobs using Spark running on an EKS cluster.

For ingesting large volumes of data, the bulk import process is preferred because the number of files written
to S3 is smaller, which means the cost for S3 PUTs is less and there is less compaction work to do later.

Note that all ingest into Sleeper is done in batches - there is currently no option to ingest the data in a way
that makes it immediately available to queries. There is a trade-off between the latency of data being visible and
the cost, with lower latency generally costing more.

## What ingest rate does Sleeper support?

In theory, an arbitrary number of ingest jobs can run simultaneously. If the limits on your AWS account allowed
it, you could have 100 EMR clusters each running a job to import 10 billion records. Each job will be writing
files to S3, and when it is finished the state store will be updated. All of these operations are independent.
Therefore the only limit is the capacity of the S3 bucket to receive data and the capacity of the DynamoDB-based
state store to receive PUTs. Thus if the 100 bulk import jobs complete at roughly the same time, the number of
records in the table would increase by 1 trillion very quickly.

However, in order for a query to return results quickly, there needs to be a modest number of files in each
partition. If there are around 10 files in a partition, then queries will be quick. In the above example,
100 files would be added to each partition. A query for a key that ran immediately after those 100 jobs
finished would have to open all 100 files, and this would mean the query would be slow.
The conpaction process will run multiple compaction jobs to compact those 100 files together into a
smaller number of files. Once this is done, queries will be quick.

This example shows that ingest is a balancing act between adding data quickly and maintaining query performance.
If too many import jobs finish in a short period then query performance will suffer. A small number of large
import jobs is better than a large number of small jobs.

## Standard Ingest

Sleeper's standard ingest process is based around the class `sleeper.ingest.IngestRecords`. This contains a
method that can be called on any `Iterable` of `Record`s. This process reads a large batch of records from the
`Iterable` and prepares it for writing into the Sleeper table. This involves: reading a batch of records
into memory, sorting them by key and sort fields, and then writing that batch to a local file. This is
repeated some number of times. At this point the data is only written locally and it is not inside
the Sleeper table. These local files are read into a sorted iterable and the data is written to files in
the S3 bucket for the table. One file is written for each of the leaf partitions. Once this is done,
these new files are added to the state store. At this point the data is available for query.

In this process there is a trade-off between the cost and the delay before data is available for query.
Each batch of records has to be written to files in S3, one per leaf partition. Suppose there were
10,000 records in the batch (i.e. the close() method was called on the ingest method after 10,000
records had been added), and suppose the table was split into 100 leaf partitions. Then each write to
S3 would be of a file containing 100 records. To write these 10,000 records would require 100 S3 PUTs
and 100 DynamoDB PUTs. Writing 10,000 records like this would have almost negligible cost. However,
writing 100 million records with batches of 10,000 records, would cause 1 million S3 PUTs which costs
around $5 and $1.25 in DynamoDB PUT costs. If the batch was 10 million records, then the cost of the
required S3 PUTs would be $0.005 and $0.00125 for DynamoDB costs. Therefore in general larger batches
are better, but there is a longer delay before the data is available for query. 

To make it easy for users to ingest data from any language, and to deploy ingest jobs in a scalable way,
there is an ingest stack of cloud components. This requires the user to write data to Parquet files,
with columns matching the fields in your schema (note that the fields in the schema of the Parquet file
all need to be non-optional). 

Note that the descriptions below describe how data in Parquet files can be ingested by sending ingest job
definitions in JSON form to SQS queues. In practice it may be easier to use the [Python API](08-python-api.md).

When you have the data you want to ingest stored in Parquet files, a message should be sent
to Sleeper's ingest queue telling it that the data should be ingested. This message should have the following form:

```JSON
{
  "id": "a_unique_id",
  "tableName": "myTable",
  "files": [
    "databucket/file1.parquet",
    "databucket/file2.parquet"
  ]
}

```

Here the items listed under `files` can be either files or directories. If they are directories, then Sleeper
will recursively look for files ending in `.parquet` within them.

It is up to you to spread the data you want to ingest over an appropriate number of jobs. As a general rule,
aim for at least 10s of millions of records per job.

The id field will be used in logging so that users can see the progress of particular ingest jobs by viewing the
logs. The URL of the SQS queue that the message should be sent to can be found from the `sleeper.ingest.job.queue.url`
property. This will be populated in the config object in the `sleeper-<instance-id>-config` S3 bucket. It can also
be found using the [admininstration client](06-status.md#Sleeper Administration Client).

You will need to ensure that the role with the ARN given by the `IngestContainerRoleARN` property has read access
to the files you wish to ingest. This ARN is exported as a named export from CloudFormation with name
`<sleeper-id>-IngestContainerRoleARN` to help stacks that depend on Sleeper automatically grant read access to their
data to Sleeper's ingest role. A simple way to do this is to use the `sleeper.ingest.source.bucket` instance property to
set the name of the bucket that the files are in. If this property is populated when the Sleeper instance is deployed then
the ingest roles will be granted read access to it. (The bulk import methods described below will also be granted read access
to it.)

Once the message has been sent to the SQS, a lambda will notice that there are messages on the queue and then start
a task on the ingest ECS cluster (this cluster is called `sleeper-<instance-id>-ingest-cluster`). This task will
then ingest the data. This process is asynchronous, i.e. it may be several minutes before the data has been ingested.

## Bulk Import

Bulk importing data into a Sleeper table means importing data by using Apache Spark to run a MapReduce-like job to
take a batch of data then partition, sort and write it out so that the resulting files can be added into a Sleeper table.
The advantage of bulk import over the standard ingest process described above is that it reduces the number of writes to S3.
For example, suppose there are currently 100 leaf partitions for a table, and suppose that we have 1000 files of data to ingest.
With the standard approach, if we create one ingest job per file and send it to the SQS queue, then there will be 100,000 writes to S3.
(In fact, there might be more if the files contain more records than the value of `sleeper.ingest.max.local.records`.) Using
the bulk import method, there will only be 100 writes to S3 (assuming that the 1000 files are all imported in the same bulk
import job).

Note that it is vital that a table is pre-split before data is bulk import ([see here](06-status.md#Reinitialise a Table)).

There are several stacks that allow data to be imported using the bulk import process:

- `EmrBulkImportStack` - this causes an EMR cluster to be deployed each time a job is submitted to the EMR bulk import queue. Each
job is processed on a separate EMR cluster. The advantage of the cluster being used for one job and then destroyed is
that there is no wasted compute if jobs are submitted infrequently. The downside is that there is a significant delay whilst
the cluster is created and bootstrapped.
- `PersistentEmrBulkImportStack` - this causes an EMR cluster to be created when the Sleeper instance is deployed. This
cluster runs continually. By default, it uses managed scaling so that the number of servers running scales up and down
as needed. The advantage of the persistent EMR approach is that if there is a continual stream of jobs coming there is no
delay while a new cluster is created (this also means the cost of the servers during the cluster creation and bootstrapping
process is amortised over multiple jobs). The downside is that if there are no jobs to perform then there is still a cost.
- `EksBulkImportStack` - this uses Spark running on an EKS cluster to bulk import the data. Currently, the executors run
as Fargate tasks. Future work will allow them to run on EC2 instances. This stack is experimental.

These can all be deployed independently of each other. Each stack has its own queue from which it pulls jobs. The
`sleeper.optional.stacks` instance property needs to include `EmrBulkImportStack`, `PersistentEmrBulkImportStack` or
`EksBulkImportStack` respectively.

#### Bulk import on EMR

The non-persistent EMR stack creates an EMR cluster when you want to run a job. When a job is
submitted a cluster is created with a small number of instances. EMR managed auto-scaling is enabled for this cluster.

A simple example of the message to send is:

```JSON
{
  "tableName": "my-table",
  "files": [
  	"my-bucket/my-files/"
  ]
}
```

This message needs to be sent to the queue with URL given by the value of the property `sleeper.bulk.import.emr.job.queue.url`.

You can configure the instance type of the nodes, as well as the initial and maximum number of core nodes in your cluster.
Default values of these can be specified in the instance properties. These can be overridden for each table by editing
the table properties. Alternatively they can be specified on a per-job basis by editing the `platformSpec` part of the
job specification:

```JSON
{
  "tableName": "my-table",
  "files": [
  	"my-bucket/my-files/"
  ],
  "platformSpec": {
  	"sleeper.table.bulk.import.emr.master.instance.type": "m5.xlarge",
  	"sleeper.table.bulk.import.emr.executor.instance.type": "m5.4xlarge",
  	"sleeper.table.bulk.import.emr.executor.initial.instances": "2",
  	"sleeper.table.bulk.import.emr.executor.max.instances": "10"
  }
}
```

When you submit your JSON job via the SQS Queue, an EMR cluster should appear in the EMR part of the AWS console with your
desired configuration. Once the cluster initialises (around 10 minutes), you will be able to follow the links in the EMR
console to access your Spark UI and application master UI. These will allow you to monitor your job and view logs from the
Spark executors and driver. After your job finishes the cluster terminates.

There are many configuration options that can be specified to control properties of the EMR cluster and the Spark configuration.
The following properties are instance properties that can be overridden by table properties and by using the `platformSpec`
part of the job specification:

```properties
sleeper.default.bulk.import.emr.release.label=emr-6.8.0 # The EMR release label to be used when creating an EMR cluster for bulk importing data using Spark running on EMR. This default can be overridden by a table property or by a property in the bulk import job specification.
sleeper.default.bulk.import.emr.master.instance.type=m5.xlarge # The EC2 instance type to be used for the master node of the EMR cluster.
sleeper.default.bulk.import.emr.executor.instance.type=m5.4xlarge # The EC2 instance type to be used for the executor nodes of the EMR cluster.
sleeper.default.bulk.import.emr.executor.initial.instances=2 # The initial number of EC2 instances to be used as executors in the EMR cluster.
sleeper.default.bulk.import.emr.executor.max.instances=10 # The maximum number of EC2 instances to be used as executors in the EMR cluster.
```

The following options can be specified in the table properties. For jobs importing data to a particular table these values
will be used instead of the default values in the instance properties, unless the values in the table properties are overridden
by properties in the job specification.

```properties
sleeper.table.bulk.import.emr.release.label=emr-6.8.0 # The EMR release label to be used when creating an EMR cluster for bulk importing data using Spark running on EMR. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.master.instance.type=m5.xlarge # The EC2 instance type to be used for the master node of the EMR cluster. This value 
overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.instance.type=m5.4xlarge # The EC2 instance type to be used for the executor nodes of the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.initial.instances=2 # The initial number of EC2 instances to be used as executors in the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
sleeper.table.bulk.import.emr.executor.max.instances=10 # The maximum number of EC2 instances to be used as executors in the EMR cluster. This value overrides the default value in the instance properties. It can be overridden by a value in the bulk import job specification.
```

#### Bulk import on persistent EMR

The persistent EMR stack creates an EMR cluster when the Sleeper instance is deployed. Bulk import jobs are run as EMR steps.
Note that this cluster will be running until you terminate it by updating the instance properties and re-runnnig the cdk deploy.
By default, EMR managed auto-scaling is enabled for this cluster. The `sleeper.bulk.import.persistent.emr.use.managed.scaling`
instance property determines whether managed scaling is used for this cluster. If this is set to false then the number of executors
in the cluster is set to `sleeper.bulk.import.persistent.emr.min.instances`. If it is set to true then the number of executors
automatically scales up and down between `sleeper.bulk.import.persistent.emr.min.instances` and `sleeper.bulk.import.persistent.emr.max.instances`.

The other properties of the cluster are controlled using similar properties to the non-persistent EMR cluster, e.g.

```properties
sleeper.bulk.import.persistent.emr.release.label=emr-6.8.0
sleeper.bulk.import.persistent.emr.master.instance.type=m5.xlarge
sleeper.bulk.import.persistent.emr.core.instance.type=m5.4xlarge
sleeper.bulk.import.persistent.emr.use.managed.scaling=true
sleeper.bulk.import.persistent.emr.min.instances=1
sleeper.bulk.import.persistent.emr.max.instances=10
```

There is an additional property `sleeper.bulk.import.persistent.emr.step.concurrency.level` that is not applicable to the
non-persistent EMR approach. This controls the number of steps that can run concurrently.

The URL of the SQS queue to which messages should be sent is given by the instance property
`sleeper.bulk.import.persistent.emr.job.queue.url`which can be found in the config
object in the bucket named `sleeper-<instance-id>-config`.

Note however that as there is one persistent EMR cluster deployed for the whole instance there are no per-table persistent
EMR properties, and it does not make sense to change the cluster properties on a per-job basis.

#### Instance properties common to the EMR and persistent EMR stacks

The following options are based on https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/.

```properties
// spark.driver options
sleeper.bulk.import.emr.spark.driver.cores=5 # The number of cores allocated to the Spark driver. Used to set spark.driver.cores.
sleeper.bulk.import.emr.spark.driver.memory=16g # The memory allocated to the Spark driver. Used to set spark.driver.memory.
sleeper.bulk.import.emr.spark.driver.extra.java.options=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' # Used to set spark.driver.extraJavaOptions.

// spark.executor options
sleeper.bulk.import.emr.spark.executor.cores=5 # The number of cores allocated to the Spark executor. Used to set spark.executor.cores.
sleeper.bulk.import.emr.spark.executor.memory=16g # The memory allocated to a Spark executor. Used to set spark.executor.memory.
sleeper.bulk.import.emr.spark.executor.heartbeat.interval=60s # Used to set spark.executor.heartbeatInterval.
sleeper.bulk.import.emr.spark.executor.instances=29 # The number of Spark executors. Used to set spark.executor.instances.
sleeper.bulk.import.emr.spark.executor.extra.java.options=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' # Used to set spark.executor.extraJavaOptions.

// spark.yarn options
sleeper.bulk.import.emr.spark.yarn.executor.memory.overhead=2g # Used to set spark.yarn.executor.memoryOverhead
sleeper.bulk.import.emr.spark.yarn.driver.memory.overhead=2g # Used to set spark.yarn.driver.memoryOverhead
sleeper.bulk.import.emr.spark.yarn.scheduler.reporter.thread.max.failures=5 # Used to set spark.yarn.scheduler.reporterThread.maxFailures

// spark.dynamicAllocation option
sleeper.bulk.import.emr.spark.dynamic.allocation.enabled=false # Used to set spark.dynamicAllocation.enabled

// spark.default.parallelism option
sleeper.bulk.import.emr.spark.default.parallelism=290 # Used to set spark.default.parallelism

// spark.memory options
sleeper.bulk.import.emr.spark.memory.fraction=0.80 # Used to set spark.memory.fraction
sleeper.bulk.import.emr.spark.memory.storage.fraction=0.30 # Used to set spark.memory.storageFraction

// spark.network options
sleeper.bulk.import.emr.spark.network.timeout=800s # Used to set spark.network.timeout
sleeper.bulk.import.emr.spark.storage.level=MEMORY_AND_DISK_SER # Used to set spark.storage.level
sleeper.bulk.import.emr.spark.rdd.compress=true # Used to set spark.rdd.compress
sleeper.bulk.import.emr.spark.shuffle.compress=true # Used to set spark.shuffle.compress
sleeper.bulk.import.emr.spark.shuffle.spill.compress=true # Used to set spark.shuffle.spill.compress

// spark.sql options
sleeper.bulk.import.emr.spark.sql.shuffle.partitions=290 # Used to set spark.sql.shuffle.partitions
```

#### Accessing YARN web interface and Spark application UI on EMR and persistent EMR clusters

To access the YARN and Spark web interfaces whilst running bulk import jobs on either the EMR or persistent EMR clusters, you
need to set the following instance properties:

```properties
sleeper.bulk.import.emr.keypair.name=my-key # An EC2 keypair to use for the EC2 instances. Specifying this will allow you to SSH to the nodes in the cluster while it's running.
sleeper.bulk.import.emr.master.additional.security.group=my-group # An EC2 Security Group. This will be added to the list of security groups that are allowed to access the
servers in the cluster.
```

You can then use the instructions [here](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html) to access the pages.
Also see [this link](https://aws.amazon.com/premiumsupport/knowledge-center/spark-driver-logs-emr-cluster/) for instructions on how to access
the logs of the Spark driver.

#### An example of using bulk import

This section describes how a large volume of records were bulk imported into Sleeper using the Persistent EMR stack. A set of 10 billion
records was generated at random. These records conformed to the schema used for the system tests, i.e. a row key of type string, a sort key of
type long, and a value of type string. The row key and the value are random strings of length 10 with characters from the lower case alphabet
a to z. The sort key is a random long in the range 0 to 10,000,000,000. The records were stored in 10 Parquet files in an prefix in an S3 bucket
(given as `mybucket/data/` in the examples below).

The table was pre-split into 256 partitions.

A persistent EMR cluster consisting of 10 core nodes of instance type m5.4xlarge and a master of type m5.xlarge was created by setting the
following instance properties:

```properties
sleeper.bulk.import.persistent.emr.release.label=emr-6.8.0
sleeper.bulk.import.persistent.emr.master.instance.type=m5.xlarge
sleeper.bulk.import.persistent.emr.core.instance.type=m5.4xlarge
sleeper.bulk.import.persistent.emr.use.managed.scaling=false
sleeper.bulk.import.persistent.emr.min.instances=10
sleeper.bulk.import.persistent.emr.step.concurrency.level=2
```

These are the default settings, with the exception of the managed scaling option.

A bulk import job was triggered by sending the following JSON to the SQS queue for the persistent EMR bulk import stack. This queue will have
the name `instance-id-BulkImportPersistentEMRQ`.

```JSON
{
  "id": "id1",
  "tableName": "my-table",
  "files": [
    "mybucket/data/"
  ]
}
```

This runs with the default settings of 29 Spark executors. The progress of the job can be tracked using the steps tab. This job took 23 minutes
to run. The rate of import would therefore be 626 billion records per 24 hours, if a sequence of these jobs was run.

To increase the rate of import, there are two options: increase the size of the cluster so that multiple jobs can run simultaneously,
or increase the size of the cluster so that a job completes quicker.

The size of the cluster was increased to 20 core servers, by using the console to manually adjust the number of servers. The number of executors
a job uses was increased to 59 using the sparkConf section of the bulk import job specification (see the section below). The job now takes 12
minutes per job. Ideally, doubling the size of the cluster would cause the job to take half the time to run. In this case it is a little worse
than half of 23 minutes, due to some overhead in starting the job up. We can see that increasing the size of the cluster has the advantage of
reducing the latency.

The size of the cluster was next increased to 40 servers. Two jobs were run in parallel using the console to set the number of
concurrent steps to 2. Each job still takes 12 minutes to run. This would give an ingest rate of 2 * 10,000,000,000 * (24 * 60) / 12 = 2.4 trillion
records per day.

#### Overriding Spark's properties for a job

The Spark properties for a job can be overridden by specifying the `sparkConf` section of the job, e.g.

```JSON
{
  "id": "id1",
  "tableName": "my-table",
  "files": [
    "mybucket/data/"
  ],
  "sparkConf": {
  	"spark.executor.instances": "29"
  }
}
```

#### Changing the bulk import class

Sleeper contains two different Spark algorithms for performing the bulk import. The first uses Dataframes and is the default
approach. The second uses RDDs. To change to the RDD approach on a per-job basis, add the following the the JSON for the job:

```JSON
"className": "sleeper.bulkimport.job.runner.rdd.BulkImportJobRDDRunner"
```

The instance property `sleeper.bulk.import.class.name` can be used to set the default algorithm.


#### Bulk import on EKS

The `EksBulkImportStack` option requires the bulk import Docker image to be pushed to ECR - see the instructions in the
[deployment guide](02-deployment-guide.md).

You can submit a job in a similar way to the methods above, e.g.

```JSON
{
    "tableName": "myTable",
    "files": [
        "my-import-bucket/files/example.parquet",
        "my-import-bucket/files/my-other-files/"
    ],
    "sparkConf": {
        "spark.executor.instances": "3",
        "spark.driver.memory": "7g",
        "spark.driver.memoryOverhead": "1g",
        "spark.executor.memory": "7g",
        "spark.executor.memoryOverhead": "1g"
        "spark.driver.cores": "1",
        "spark.executor.cores": "1"
    }
}
```

You can change the memory settings and number of executors. The settings shown are the default ones so will be
included even if you don't ask for them. It's important that your driver and executor memory and CPU settings are
compatible with [AWS Fargate's supported values](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html)
otherwise the job will fail to start. The total memory for the spark driver or executor is calculated by adding
the `spark.[driver/executor].memory` and `spark.[driver/executor].memoryOverhead` The memory overhead should be
around 10% of the executor memory. Otherwise you start to run into memory issues on Kubernetes and your nodes
will start being killed.

The Spark job will be run with a service account which has permissions to read from the bulk import bucket, and write to all the
Sleeper tables in the instance so there should be no issues with permissions.

The bulk import job will go through some initial validation, and if successful will be transformed and submitted to an AWS Step
Functions State Machine. If the job fails validation, or for some reason is unable to be submitted to the State Machine, a CloudWatch
alarm will trigger and an email will be sent to the address specified in `sleeper.errors.email`.

When the job makes it to the State Machine, it will run the job synchronously and watch its status. If the job is successful, the
job will be torn down automatically. If unsuccessful or the job doesn't submit, a notification will be sent to the errors email.

##### Debugging and UI access

While a Spark job is running you'll be able to monitor it with the Spark UI. To access this, you'll need to install `kubectl`, a command line
utility for Kubernetes. Once you've done that, have a look at the outputs of the `EksBulkImportStack`. There should be one with a value like:
`BulkImportStack.BulkImportClusterConfigCommandABCD1234 = aws eks update-kubeconfig --name ...`. Copy and paste this command into a terminal.
This will give you access to your cluster. From there you'll be able to inspect logs, list pods and connect remotely to the Spark UI. The driver
pods all use the job ID as it's name. If you don't set this manually, it will be a random UUID.

```bash
instance_id=abc1234

# This shortcut means we don't have to add -n <the namespace> to all our commands
kubectl config set-context --current --namespace sleeper-${instance_id}-bulk-import

# Inspect the logs (add -f to follow them)
kubectl logs pods/my-job-name

# Forward connections to the spark UI (type localhost:4040 into your browser to bring it up)
kubectl port-forward my-job-name 4040:4040
```

