#!/bin/bash
# Copyright 2017, bwsoft management
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

function finish {
  pkill sflowtool
}

export PYTHONPATH=$PYTHONPATH:$(pwd)
output_file="output_sflow.csv"
while [[ $# -gt 1 ]]
do
key="$1"
case $key in
    -d|--delay)
    delay="$2"
    shift # past argument
    ;;
    -t|--topic)
    topic="$2"
    shift # past argument
    ;;
    -s|--server)
    server="$2"
    shift # past argument
    ;;
    -o|--output_file)
    output_file="$2"
    shift # past argument
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done

pkill sflowtool
sflowtool -l | xargs -I {} bash -c 'echo $(date +%s%3N),{}' > "./simple_producer/${output_file}" &
python3 ./simple_producer/main.py --delay="${delay}" --topic="${topic}" --server="${server}" --continuously=="True" --data_file="./simple_producer/${output_file}"

trap finish EXIT