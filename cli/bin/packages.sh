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

echo "Building wheel..."
BASEDIR=`dirname $0`/..

if [ ! -d "$BASEDIR/env" ]; then
    virtualenv -q $BASEDIR/env --prompt='(dcos-kafka-cli) '
    echo "Virtualenv created."
fi

cd $BASEDIR
if [ "$(uname)" == "Darwin" ]; then
    source $BASEDIR/env/bin/activate
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    source $BASEDIR/env/bin/activate
elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW32_NT" ]; then
    source $BASEDIR/env/Scripts/activate
fi
echo "Virtualenv activated."
python setup.py bdist_wheel

echo "Building egg..."
python setup.py sdist

