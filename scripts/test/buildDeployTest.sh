#!/usr/bin/env bash
# Copyright 2022 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

if [ "$#" -ne 3 ]; then
	echo "Usage: $0 <uniqueId> <vpc> <subnet>"
	exit 1
fi

INSTANCE_ID=$1
VPC=$2
SUBNET=$3

TABLE_NAME="system-test"
THIS_DIR=$(cd $(dirname $0) && pwd)
PROJECT_ROOT=$(dirname $(dirname ${THIS_DIR}))


echo "-------------------------------------------------------------------------------"
echo "Building Project"
echo "-------------------------------------------------------------------------------"
pushd ${PROJECT_ROOT}/java
VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)
mvn -q clean install -Pquick
mkdir -p ${PROJECT_ROOT}/scripts/jars
mkdir -p ${PROJECT_ROOT}/scripts/docker
cp  ${PROJECT_ROOT}/java/distribution/target/distribution-${VERSION}-bin/scripts/jars/* ${PROJECT_ROOT}/scripts/jars/
cp -r ${PROJECT_ROOT}/java/distribution/target/distribution-${VERSION}-bin/scripts/docker/* ${PROJECT_ROOT}/scripts/docker/
cp  ${PROJECT_ROOT}/java/distribution/target/distribution-${VERSION}-bin/scripts/templates/version.txt ${PROJECT_ROOT}/scripts/templates/version.txt

echo "-------------------------------------------------------------------------------"
echo "Configuring Deployment"
echo "-------------------------------------------------------------------------------"
TEMPLATE_DIR=${PROJECT_ROOT}/scripts/templates
GENERATED_DIR=${PROJECT_ROOT}/scripts/generated
INSTANCE_PROPERTIES=${GENERATED_DIR}/instance.properties
TABLE_PROPERTIES=${GENERATED_DIR}/table.properties
SYSTEM_TEST_PROPERTIES=${GENERATED_DIR}/system-test.properties

mkdir -p ${GENERATED_DIR}

echo "Creating System Test Specific Instance Properties Template"
sed \
  -e "s|^sleeper.optional.stacks=.*|sleeper.optional.stacks=CompactionStack,GarbageCollectorStack,PartitionSplittingStack,QueryStack,SystemTestStack,IngestStack,EmrBulkImportStack|" \
  -e "s|^sleeper.retain.infra.after.destroy=.*|sleeper.retain.infra.after.destroy=false|" \
  ${THIS_DIR}/system-test-instance.properties > ${INSTANCE_PROPERTIES}
sed \
  -e "s|^sleeper.systemtest.repo=.*|sleeper.systemtest.repo=${INSTANCE_ID}/system-test|" \
  ${THIS_DIR}/system-test.properties > ${SYSTEM_TEST_PROPERTIES}

echo "THIS_DIR: ${THIS_DIR}"
echo "PROJECT_ROOT: ${PROJECT_ROOT}"
echo "TEMPLATE_DIR: ${TEMPLATE_DIR}"
echo "VERSION: ${VERSION}"
echo "GENERATED_DIR: ${GENERATED_DIR}"
echo "INSTANCE_PROPERTIES: ${INSTANCE_PROPERTIES}"

cp -r "${PROJECT_ROOT}/java/system-test/docker" "${PROJECT_ROOT}/scripts/docker/system-test"
cp -r "${PROJECT_ROOT}/java/system-test/target/system-test-${VERSION}-utility.jar" "${PROJECT_ROOT}/scripts/docker/system-test/system-test.jar"

echo "Starting Pre-deployment steps"
${PROJECT_ROOT}/scripts/deploy/pre-deployment.sh ${INSTANCE_ID} ${VPC} ${SUBNET} ${TABLE_NAME} ${TEMPLATE_DIR} ${GENERATED_DIR}

echo "-------------------------------------------------------------------------------"
echo "Deploying System Stack"
echo "-------------------------------------------------------------------------------"
cdk -a "java -cp ${JAR_DIR}/cdk-${VERSION}.jar sleeper.cdk.SleeperCdkApp" deploy \
--require-approval never -c propertiesfile=${INSTANCE_PROPERTIES} -c validate=true "*"

${PROJECT_ROOT}/scripts/deploy/connectToTable.sh ${INSTANCE_ID} ${TABLE_NAME}

echo "-------------------------------------------------------------------------------"
echo "Deploying System Test Stack"
echo "-------------------------------------------------------------------------------"
cdk -a "java -cp ${PROJECT_ROOT}/java/system-test/target/system-test-*-utility.jar sleeper.systemtest.cdk.SystemTestApp" deploy \
--require-approval never -c instancepropertiesfile=${INSTANCE_PROPERTIES} -c validate=true "*" \
-c tablepropertiesfile=${TABLE_PROPERTIES} -c testpropertiesfile=${SYSTEM_TEST_PROPERTIES}

echo "-------------------------------------------------------------------------------"
echo "Writing Random Data"
echo "-------------------------------------------------------------------------------"
CONFIG_BUCKET=$(cat ${GENERATED_DIR}/configBucket.txt)
java -cp ${PROJECT_ROOT}/java/system-test/target/system-test-*-utility.jar \
sleeper.systemtest.ingest.RunWriteRandomDataTaskOnECS ${INSTANCE_ID} system-test
