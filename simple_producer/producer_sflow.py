import random
from enum import Enum
import time
from kafka import KafkaProducer


class ProducerStatus(Enum):
    Created = 1
    Running = 2
    Stopped = 3

class ProducerSFLOW:
    def __init__(self, delay, topic, servers='localhost:29092', data_file="", random_data=False, file_ip="",
                 continuously=False):
        self._path_to_file = data_file
        self._delay = delay
        self.status = ProducerStatus.Created
        if self._path_to_file:
            self._file = open(data_file, "r")
        self._topic = topic
        self._producer = KafkaProducer(bootstrap_servers=servers)
        self._continuously = continuously
        self._random_data = random_data
        if self._random_data:
            self._random_sflow = GeneratingRandomSFLOW(file_ip)

    def __del__(self):
        if self._path_to_file:
            self._file.close()

    def run(self):
        self.status = ProducerStatus.Running
        self._send_to_kafka()

    def _follow(self):
        if self._continuously:
            while self.status == ProducerStatus.Running:
                line = self._file.readline().rstrip()
                if not line:
                    time.sleep(self._delay)
                    continue
                yield line
        elif self._path_to_file:
            for line in self._file:
                if line:
                    time.sleep(self._delay)
                    yield line
        elif self._random_data:
            while self.status == ProducerStatus.Running:
                line = self._random_sflow.get_rand_record_sflow()
                time.sleep(self._delay)
                yield line

    def _send_to_kafka(self):
        sflow_lines = self._follow()
        for line in sflow_lines:
            self._producer.send(self._topic, str.encode(line))

    def stop(self):
        self.status = ProducerStatus.Stopped

class GeneratingRandomSFLOW:
    def __init__(self, path_to_file_ip):
        file = open(path_to_file_ip, "r")
        self._ip = []
        for line in file:
            self._ip.append(line.rstrip())
        file.close()
        self.status = ProducerStatus.Created

    def get_rand_record_sflow(self):
        n = len(self._ip)
        str_record = ''
        str_record = str_record + str(random.randint(1184794182, 1784794182)) + ','  # 1 timestamp
        str_record = str_record + 'FLOW' + ','  # 2 flow_indicator
        str_record = str_record + self._ip[random.randint(0, n - 1)] + ','  # 3 agent_address
        str_record = str_record + str(random.randint(0, 65000)) + ','  # 4 input_port
        str_record = str_record + str(random.randint(0, 65000)) + ','  # 5 output_port
        str_record = str_record + '0025907b2121' + ','  # 6 src_mac
        str_record = str_record + '003048f17b1f' + ','  # 7 dst_mac
        str_record = str_record + '0x0800' + ','  # 8 ethernet_type
        str_record = str_record + str(random.randint(0, 1000)) + ','  # 9 in_vlan
        str_record = str_record + str(random.randint(0, 1000)) + ','  # 10 out_vlan
        str_record = str_record + self._ip[random.randint(0, n - 1)] + ','  # 11 src_ip
        str_record = str_record + self._ip[random.randint(0, n - 1)] + ','  # 12 dst_ip
        str_record = str_record + str(random.randint(0, 10)) + ','  # 13 ip_protocol
        str_record = str_record + '0x08' + ','  # 14 ip_tos
        str_record = str_record + '64' + ','  # 15 ip_ttl
        str_record = str_record + str(random.randint(0, 65000)) + ','  # 16 src_port_or_icmp_type
        str_record = str_record + str(random.randint(0, 65000)) + ','  # 17 dst_port_or_icmp_code
        str_record = str_record + '0x18' + ','  # 18 tcp_flags
        str_record = str_record + str(random.randint(0, 65000)) + ','  # 19 packet_size
        str_record = str_record + str(random.randint(0, 1000)) + ','  # 20 ip_size
        str_record = str_record + '512'  # 21  sampling_rate
        return str_record
