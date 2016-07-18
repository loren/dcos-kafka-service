#
#    Copyright (C) 2016 Mesosphere, Inc.
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
from dcos_kafka import kafka_utils as ku


def connection():
    ku.http_get_json("/connection")


def connection_address():
    ku.http_get_json("/connection/address")


def connection_dns():
    ku.http_get_json("/connection/dns")
