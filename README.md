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
            1.2.1. Switch to the swarm mode
            1.2.2. Create a network
            1.2.3. Run Zookeeper
            1.2.4. Run Kafka
            1.2.5. Run sflow-producer 
            1.2.6. Run Grafana
            1.2.7. Run Influxdb
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
Before starting the application, you need to deploy the following services:

* Kafka + Zookeeper
* Grafana
* InfluxDB

### Dependencies
To download necessary docker images, run the following commands:

* InfluxDB: `docker pull influxdb`
* Zookeeper: `docker pull confluentinc/cp-zookeeper:3.0.0`
* Kafka: `docker pull confluentinc/cp-kafka:3.0.0`
* Grafana: `docker pull grafana/grafana`

Additionally, in order for functions country(), city() and aarea() to work, you need to download geolite2 databases from the following page:
`https://dev.maxmind.com/geoip/geoip2/geolite2/`

Sflow-producer is a utility program for sending sflow data to Kafka. To build a docker image for it, run the following commands: `docker build -t sflow-producer sflow-producer/`

### Deployment
* Switch to the swarm mode: `docker swarm init`
* Create an overlay network: `docker network create --driver overlay --attachable=true network-name`
* Run Zookeeper: `docker service create --name=zookeeper --mount type=bind,source=/path/to/folder,destination=/var/lib/zookeeper/data -e ZOOKEEPER_CLIENT_PORT=32181 -e ZOOKEEPER_TICK_TIME=2000 --network=network-name confluentinc/cp-zookeeper:3.0.0`
* Run Kafka: `docker service create --name=kafka --mount type=bind,source=/path/to/folder,destination=/var/lib/kafka/data -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:32181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092 --network=network-name confluentinc/cp-kafka:3.0.0`
* Run sflow-producer: `docker run -d --rm --name=producer -e TOPIC=topic_name -e ZK=zookeeper:32181 -e BROKER=kafka:29092 -p 6343:6343/udp --network=network-name sflow-producer`
* Run Grafana: `docker service create --name=sflow-view -p 3000:3000 --env GF_SECURITY_ADMIN_PASSWORD=your_password --network=network-name grafana/grafana`
    
        login: admin, password: your_password
    
* Run InfluxDB: `docker service create --name=sflow-store --mount type=bind,source=/path/to/folder,destination=/var/lib/influxdb --network=network-name influxdb`
    
        login: root, password: root

## Configuration
To run the application, you will need a configuration file. It is a json file with the following structure:
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
File has 5 sections: 4 of them are mandatory (input, output, processing, databases) and 1 is optional (analysis). Sections are described in more detail below (all fields are mandatory).

### Section input
This section describes how the application will receive data.

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
This section describes how the application will output data.

* "method" - data output type, valid values: "csv", "influx"
* "options" > influx":
    * "host" - influxdb hostname
    * "port": influxdb port
    * "username": influxdb user
    * "password": password for user above
    * "database": influxdb database name
    * "measurement": measurement name

### Section processing
This section specifies transformations and aggregations to be performed on the input data.

* "transformation" - this string specifies transformation steps for every string received from the input source.

   User can rename the field (new_name: old_name), apply one of 7 functions: sum, div, mult, minus, country, city, aarea (src_country: country(src_ip)) or just use the field unchanged. 

   Nested operations (sum(mult(packet_size, sampling_rate), 1000)) are not supported at the moment. 

   Each field declared in the transformation section should be subsequently used in aggregation, otherwise the application will raise exception.
   
* aggregation - this section specifies how the data should be aggregated after transformation.
    
  Field "operation_type" specifies aggregation type, valid values are "reduce" and "reduceByKey". In case of "reduceByKey" user needs to specify an aggregation key (key = src_country).
  
  The key can contain more than one field (key = (src_ip, dst_country, src_port_or_icmp_type)).
  
  User can specify one of the 4 aggregation functions: Sum, Mult, Max, Min (Max(packet_size)).

### Section databases
This section specifies paths to databases which are necessary for the geo functions (country(), city(), aarea()) to work.

* "country" - path to database for resolving country 
* "city" - path to database for resolving city
* "asn" - path to database for resolving asn

### Section analysis
This section specifies rules for data analysis and ways to notify about detected anomalies.

* Section "historical" is mandatory at the moment. It specifies that analysis will be based on historical data.
    * "method" - source of historical data, valid values: "influx"
    * "influx_options" - see section output > options
    
* Section "alert" specifies settings for notifications of detected anomalies.
    * "method" -specifies output method for notifications, valid values: "stdout", "kafka"
    * "options"
        * "server" - kafka hostname
        * "port" - kafka port
        * "topic" - kafka topic

* accuracy - accuracy in seconds, [ now - time_delta - accuracy; now - time_delta + accuracy ]

* Section "rule" contains an array of user-defined analysis modules with their respective names and options. System automatically imports class "SimpleAnalysis", so you don’t need to explicitly specify it.
    * "module" - name of the class to be used for analysis. Specified class should be located in a folder with the same name and needs to implement the IUserAnalysis interface. Method with name "analysis" should be implemented. This method will receive two arguments. First argument is an object which provides historical data access by index and field name. Second argument is an object which allows to send notifications by calling its method "send_message"
    * "name" - module name to be used in warning messages
    * "options" - settings to be passed to the class constructor. These are user defined and allow control over analysis behaviour

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
When the infrastructure is deployed and the configuration file is ready, you can run the application. To build a docker image for the application run the following commands:

* Build a base image for spark: `docker build -t bw-sw-spark docker/`
* Build an image for the application: `docker build -t sflow-app .`
* Run the application: `docker service create --name=app --mount type=bind,source=$PWD,destination=/configs --network=network-name sflow-app /configs/config_reducebykeys.json`
    
