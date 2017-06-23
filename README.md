# Table of Contents
    1. Infrastructure
        1.1. Dependencies
            1.1.1. InfluxDB
            1.1.2. Zookeeper
            1.1.3. Kafka
            1.1.4. Grafana
            1.1.5. GeoLite2 
            1.1.6. Sflow-producer
        1.2. Deployment
            1.2.1. Switch to swarm mode
            1.2.2. Create network
            1.2.3. Running Zookeeper
            1.2.4. Running Kafka
            1.2.5. Running sflow-producer 
            1.2.6. Running Grafana
            1.2.7. Running Influxdb
    2. Configuration
        2.1. Sections description
            2.1.1. Section "input"
            2.1.2. Section "output"
            2.1.3. Section "processing"
            2.1.4. Section "databases"
            2.1.5. Section "analysis"
    3. List of fields for transformations
    4. Running application

## Infrastructure
For correct application working we must deploy following services:

* Kafka + Zookeeper
* Grafana
* InfluxDB

### Dependencies
For downloading according docker images run do following:

* InfluxDB: `docker pull influxdb`
* Zookeeper: `docker pull confluentinc/cp-zookeeper:3.0.0`
* Kafka: `docker pull confluentinc/cp-kafka:3.0.0`
* Grafana: `docker pull grafana/grafana`

Furthermore, for correct working functions country, city and aarea you should download geolite2 databases by following link: 
`https://dev.maxmind.com/geoip/geoip2/geolite2/`

Sflow-producer it's util for sending sflow data to kafka, for build docker image run command: `docker build -t sflow-producer sflow-producer/`

### Deployment

* Switch to swarm mode: `docker swarm init`
* Create overlay network: `docker network create --driver overlay --attachable=true network-name`
* Running Zookeeper: `docker service create --name=zookeeper --mount type=bind,source=/path/to/folder,destination=/var/lib/zookeeper/data -e ZOOKEEPER_CLIENT_PORT=32181 -e ZOOKEEPER_TICK_TIME=2000 --network=network-name confluentinc/cp-zookeeper:3.0.0`
* Running Kafka: `docker service create --name=kafka --mount type=bind,source=/path/to/folder,destination=/var/lib/kafka/data -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:32181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092 --network=network-name confluentinc/cp-kafka:3.0.0`
* Running sflow-producer: `docker run -d --rm --name=producer -e TOPIC=topic_name -e ZK=zookeeper:32181 -e BROKER=kafka:29092 -p 6343:6343/udp --network=network-name sflow-producer`
* Running Grafana: `docker service create --name=sflow-view -p 3000:3000 --env GF_SECURITY_ADMIN_PASSWORD=your_password --network=network-name grafana/grafana`
    
        login: admin, password: your_password
    
* Running InfluxDB: `docker service create --name=sflow-store --mount type=bind,source=/path/to/folder,destination=/var/lib/influxdb --network=network-name influxdb`
    
        login: root, password: root


## Configuration
For run application we need configuration file. Configuration file it's json file with following struct:
```json
{
    "input": {
        "input_type": "kafka",
        "data_structure": "config_data_structure.json",
        "options": {
          "server": "zookeeper",
          "port": 32181,
          "consumer_group": "sflow-consumer",
          "topic": "test",
          "batchDuration": 10,
          "sep": ","
        }
    },
    "output": {
        "method": "influx",
        "options": {
            "influx": {
                "host":"sflow-store",
                "port": 8086,
                "username": "root",
                "password": "root",
                "database": "dev",
                "measurement": "points"
            }
        }
    },
    "processing": {
        "transformation": "src_country: country(src_ip);packet_size;traffic: mult(packet_size,sampling_rate)",
        "aggregations": {
            "operation_type": "reduceByKey",
            "rule": "key = src_country; Max(packet_size); Sum(traffic)"
        }
    },
    "analysis": {
        "historical": {
            "method": "influx",
            "influx_options": {
                "host": "localhost",
                "port": 8086,
                "username": "root",
                "password": "root",
                "database": "test3",
                "measurement": "points"
            }
        },
        "alert": {
            "method": "kafka",
            "option": {
                "server": "localhost",
                "port": 29092,
                "topic": "testalert"
            }
        },
        "accuracy": 3,
        "rule": [
            {
                "module": "SimpleAnalysis",
                "name": "SimpleAnalysis1",
                "options": {
                    "deviation": {
                        "packet_size": 5,
                        "traffic": 8
                    },
                    "batch_number": 1
                }
            },
            {
                "module": "SimpleAnalysis",
                "name": "SimpleAnalysis2",
                "options": {
                    "deviation": {
                        "packet_size": 5,
                        "traffic": 3
                    },
                    "batch_number": 3
                }
            },
            { 
                "module": "AverageAnalysis",
                "name": "AverageAnalysis",
                "options": {
                    "deviation": { "traffic": 4 },
                    "num_average": 3
                }
            }
        ]
    },
    "databases": {
        "country": "./GeoLite2-Country.mmdb",
        "city": "./GeoLite2-City.mmdb",
        "asn": "./GeoLite2-ASN.mmdb"
    }
}
```
In file 5 sections: 4 required ( input, output, processing, databases ) and 1 optional (analysis). Describe every section below in details (all fields is required):

### Section input
This section describes how application receive data

* "input_type" - input data type, valid values: "csv", "kafka"
* "data_structure" - path to file with data structure 
* "options":
    * "server" - zookeeper hostname
    * "port" - zookeeper port
    * "consumer_group" - kafka consumer group
    * "topic" - kafka topic
    * "batchDuration" - data sampling window in seconds
    * "sep" - fields delimiter for received string

### Section output
This section describes how application deduces data

* "method" - data output type, valid values: "csv", "influx"
* "options" > influx":
    * "host" - influxdb hostname
    * "port": influxdb port,
    * "username": influxdb user,
    * "password": password for user above,
    * "database": influxdb database name,
    * "measurement": measurement name

### Section processing
This section describes transformations and aggregations which will be preformed on input data

* "transformation" - this string describes how will be transformed every received string from kafka.  

   User can rename field (new_name: old_name), apply one of 7 functions: sum, div, mult, minus, country, city, aarea (src_country: country(src_ip)) or just use field without changes.  

   Now nested operations (sum(mult(packet_size, sampling_rate), 1000)) not supported. 

   Each field from transformation should be used in aggregation, otherwise the program will raise exception.
   
* aggregation - this section describes how will aggregate transformed data.  
    
  Field "operation_type" is aggregation type, valid values is "reduce" or "reduceByKey". In case "reduceByKey" user should specify aggregation key (key = src_country).
  
  Key can contains more than one field (key = (src_ip, dst_country, src_port_or_icmp_type)).
  
  User can apply to data one of 4 aggregation function: Sum, Mult, Max, Min (Max(packet_size)).

### Section databases
This section contains paths to databases which necessary to work functions country, city, aarea

* "country" - path to database, for resolving country 
* "city" - path to database, for resolving city
* "asn" - path to database, for resolving asn

### Section analysis
Rules for analysis and notifications about anomalies

* Section "historical" now required. This say that analysis should be based on historical data
    * "method" - source for historical data, valid values: "influx"
    * "influx_options" - see section output > options
    
* Section "alert" describes settings for anomalies notifications
    * "method" - type of notifications output, valid values: "stdout", "kafka"
    * "options"
        * "server" - kafka hostname
        * "port" - kafka port
        * "topic" - kafka topic

* accuracy - accuracy in seconds, [ now - time_delta - accuracy; now - time_delta + accuracy ]

* Section "rule" contains array analysis modules names and options for them
    * "module" - class name for analysis. For example in analysis module will import class "SimpleAnalysis". 
    This class should be in folder with same name and implement IUserAnalysis interface. Method with name "analysis" should be implement. This method receive two arguments.
    First argument is object which allow get historical data by index and field name. Second argument is object which allow send notification by calling method "send_message"
    * "name" - module name which use for output warning messages
    * "options" - settings which will send to class constructor. This is user defined options and allow control analysis behaviour

## List of fields for transformations
```
   'timestamp',
   'FLOW_indicator',
   'agent_address',
   'input_port',
   'output_port',
   'src_mac',
   'dst_mac',
   'ethernet_type',
   'in_vlan',
   'out_vlan', 
   'src_ip', 
   'dst_ip', 
   'ip_protocol', 
   'ip_tos', 
   'ip_ttl', 
   'src_port_or_icmp_type' 
   'dst_port_or_icmp_code' 
   'tcp_flags', 
   'packet_size', 
   'ip_size',
   'sampling_rate

```

## Running application

When infrastructure and config ready we can run application. For building image with application run following commands:

* Build base image with spark: `docker build -t bw-sw-spark docker/`
* Build application image: `docker build -t sflow-app .`
* Run application: `docker service create --name=app --mount type=bind,source=$PWD,destination=/configs --network=network-name sflow-app /configs/config_reducebykeys.json`
    
