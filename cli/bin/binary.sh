#!/bin/bash -e
#
#    Copyright (C) 2015 Mesosphere, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Building binary..."
pyinstaller binary/binary.spec
pyinstaller binary/binary-confluent.spec

docker-check() {
  time=2
  command="/bin/sh -c \"docker ps\""

  if hash expect 2>/dev/null; then
    expect -c "set echo \"-noecho\"; set timeout $time; spawn -noecho $command; expect timeout { exit 1 } eof { exit 0 }"

    if [ $? = 1 ] ; then
      echo "Docker execution timed out. Make sure docker-machine start docker-vm is started."
      exit 0;
    fi
   fi
}

if [ "$(uname)" == "Darwin" ]; then
    # Do something under Mac OS X platform
    mkdir -p dist/darwin
    mv dist/dcos-kafka dist/darwin
    mv dist/dcos-confluent dist/darwin
    shasum -a 256 dist/darwin/dcos-kafka | awk '{print $1}' > dist/darwin/dcos-kafka.sha
    shasum -a 256 dist/darwin/dcos-confluent | awk '{print $1}' > dist/darwin/dcos-confluent.sha1
    echo "Darin Build Complete!"

    # linux build on a darwin plaform if docker runs
    docker-check

    docker rmi -f kafka-binary || true
    docker rm kafka-binary || true
    docker build -f binary/Dockerfile.linux-binary -t kafka-binary .
    docker run --name kafka-binary kafka-binary
    mkdir -p dist/linux
    docker cp kafka-binary:/dcos-kafka/dist/linux dist/

elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
      # Do something under GNU/Linux platform  #statements
      mkdir -p dist/linux
      mv dist/dcos-kafka dist/linux
      mv dist/dcos-confluent dist/linux
      sha256sum dist/linux/dcos-kafka | awk '{print $1}' > dist/linux/dcos-kafka.sha
      sha256sum dist/linux/dcos-confluent | awk '{print $1}' > dist/linux/dcos-confluent.sha1
      echo "Linux Build Complete"
fi

echo "Build finished!"
