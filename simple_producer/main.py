import getopt
import time
from errno import EINTR

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
        elif opt in ("-f", "--server"):
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
        elif data_file:
            producer_sflow = ProducerSFLOW(delay, topic, server, data_file=data_file)
        elif continuously and data_file:
            producer_sflow = ProducerSFLOW(delay, topic, server, data_file=data_file, continuously=True)

        try:
            producer_sflow.run()
        except KeyboardInterrupt:
            producer_sflow.stop()
    else:
        print("Error: set delay,topic and server")
