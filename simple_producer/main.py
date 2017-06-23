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

import getopt

from simple_producer.producer_sflow import ProducerSFLOW
import sys

if __name__ == "__main__":
    argv = sys.argv[1:]
    delay = 0
    topic = ''
    server = ''
    file_ip = ''
    random_data = False
    continuously = False
    data_file = ''

    try:
        opts, args = getopt.getopt(argv, "hd:t:s:f:r:c:df", ["delay=", "topic=", "server=", "file_ip=", "random_data=",
                                                             "continuously=", "data_file="])
    except getopt.GetoptError:
        print("Error in argument of call")
        print("Example: python main.py -d 1 -t 'test1' -s 'localhost:29092' -f 'ip.txt'")
        print("Example: python main.py --delay=1 --topic='test1' --server='localhost:29092' --file_ip='ip.txt' "
              "--random_data=True")
        print("additional parameters  --data_file='sflow.csv' --continuously=True")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("Example: python main.py -d 1 -t 'test1' -s 'localhost:29092' -f 'ip.txt' -d True")
            print("Example: python main.py --delay=1 --topic='test1' --server='localhost:29092' --file_ip='ip.txt "
                  "--random_data=True'")
            print("additional parameters --random_data=True --data_file='sflow.csv' --continuously=True")
            sys.exit()
        elif opt in ("-d", "--delay"):
            delay = float(arg)
        elif opt in ("-t", "--topic"):
            topic = arg
        elif opt in ("-s", "--server"):
            server = arg
        elif opt in ("-f", "--file_ip"):
            file_ip = arg
        elif opt in ("-r", "--random_data"):
            random_data = bool(arg)
        elif opt in ("-c", "--continuously"):
            continuously = bool(arg)
        elif opt == "--data_file":
            data_file = arg

    if delay and topic and server:
        if random_data and file_ip:
            producer_sflow = ProducerSFLOW(delay, topic, server, random_data=True, file_ip=file_ip)
        elif continuously and data_file:
            producer_sflow = ProducerSFLOW(delay, topic, server, data_file=data_file, continuously=True)
        elif data_file:
            producer_sflow = ProducerSFLOW(delay, topic, server, data_file=data_file)
        try:
            producer_sflow.run()
        except KeyboardInterrupt:
            producer_sflow.stop()
    else:
        print("Error: set delay,topic, server and type generation sflow")
        print("Example: python main.py -d 1 -t test3 -s localhost:29092 -f ip.txt -r True")
