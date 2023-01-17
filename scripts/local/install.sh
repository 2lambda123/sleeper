#!/usr/bin/env bash

# Copyright 2022-2023 Crown Copyright
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

if [ "$#" -lt 1 ]; then
	echo "Usage: install.sh <version>"
	exit 1
fi

VERSION=$1

docker pull ghcr.io/gchq/sleeper-local:$VERSION

sudo curl "https://raw.githubusercontent.com/gchq/sleeper/$VERSION/scripts/local/runInDocker.sh" --output /usr/local/bin/sleeper
sudo chmod a+x /usr/local/bin/sleeper
