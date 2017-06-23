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

export PYTHONHASHSEED=0
arg_app=$*
echo "arg = $arg_app"
arg_app=""
repack=false

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --master)
    master="$2"
    shift # past argument
    ;;
    --dependencies)
    dependencies="$2"
    shift # past argument
    ;;
    --repack)
    repack="$2"
    shift # past argument
    ;;
    --help)
    echo "example: ./spark-submit-sflow.sh --master spark://192.168.1.1:7077 --dependencies tmp main.py config.json"
    echo "set --repack true if you want repack exist packages"
    exit # past argument
    ;;
    *)
    arg_app+=$key" "       # unknown option
    ;;
esac
shift # past argument or value
done

if [ -z ${master} ]
then
    echo "Error: set ip master use option --master"
    exit
fi
if [ -z ${dependencies} ]
then
    echo "Error: set folder for dependencies use option --dependencies"
    exit
fi

declare -A arr
arr[kafka]="https://pypi.python.org/packages/3a/40/50dec31d2e6dbb6dd31104b02fe4141e0f3e1b26f0e66ebf609284b6c94f/kafka-1.3.3.tar.gz#md5=7d21408c800fd5fb106ece49a3fe0b45"
arr[influx]="https://pypi.python.org/packages/82/71/5026237edb11b01b717d74fdd8023177257731276a954cd05f825795cb60/influxdb-4.1.0.tar.gz#md5=5cd4bbbf8209d70ef80135c647674bea"
arr[nanotime]="https://pypi.python.org/packages/d5/54/6d5924f59cf671326e7809f4b3f70fa8df535d67e952ad0b6fea02f52faf/nanotime-0.5.2.tar.gz#md5=060b9dbf8dd21631a31b65040809366e"
arr[geoip2]="https://pypi.python.org/packages/4a/de/0502344a1ddcda501c1498607e0f5eb036ee1159419d9e08d269b0f23ada/geoip2-2.5.0.tar.gz#md5=61f02953fb255ff5b8944f72f9e6cd6f"
arr[maxminddb]="https://pypi.python.org/packages/f8/7a/bac498d2b7491dd8b837dff8831a7e7628db72c4b840b54743961a8f0454/maxminddb-1.3.0.tar.gz#"
arr[pytz]="https://pypi.python.org/packages/a4/09/c47e57fc9c7062b4e83b075d418800d322caa87ec0ac21e6308bd3a2d519/pytz-2017.2.zip#md5=f89bde8a811c8a1a5bac17eaaa94383c"
arr[dateutil]="https://pypi.python.org/packages/5b/11/246237ce2a1dd87ffebef0e430033f877b31dd208b281914d4fd3c531ee7/dateutils-0.6.6.tar.gz#md5=2ba7fcac03635f1f1cad0d94d785001b"

tmp_path=tmp
base_path=$(pwd)
mkdir -p ${dependencies}
mkdir -p ${tmp_path}

if [[ ! -d 'dist' ]] || [[ $repack == 'true' ]]
then
    python setup.py bdist_egg
    name_egg_app=$(cat setup.py | grep name | sed -e 's/\(^.*\"\)\(.*\)\(\".*$\)/\2/')
    echo "name_egg_app=$name_egg_app"
    egg_name=$(find dist -name "$name_egg_app*" -print0 | xargs -0 ls)
    echo "egg_name=$egg_name"
    cp $egg_name $base_path"/"$dependencies
fi

cd ${tmp_path}
echo $(pwd)
declare -a file_names

for url in "${arr[@]}"
do
    file_name=$(echo ${url} | sed -e 's/\(^.*\/\)\(.*\)\(\#.*$\)/\2/')
    file_names+=($file_name)
    if [ -f $file_name ]; then
        echo "File $file_name exists."
    else
        wget ${url}
    fi
done

for file in "${file_names[@]}"
do

    if [[ $file == *"zip"* ]]; then
        unzip -oq $file
        base_name=${file%.zip}
    else
        tar -xzf $file
        base_name=${file%.tar.gz}
    fi
    if [[ ! -d $base_name"/dist" ]] || [[ $repack == 'true' ]]
    then
        cd $base_name
        echo "base_name=$base_name"
        python setup.py bdist_egg
        egg_name=$(find dist -name "$base_name*" -print0 | xargs -0 ls)
        echo "egg_name=$egg_name"
        cp $egg_name $base_path"/"$dependencies
        cd ..
    fi
done

cd ..

echo $(pwd)
echo $arg_app
spark-submit --deploy-mode client --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 \
--py-files $dependencies/sflow_analysis-1.0.0-py3.5.egg,$dependencies/kafka-1.3.3-py3.5.egg,\
$dependencies/influxdb-4.1.0-py3.5.egg,$dependencies/nanotime-0.5.2-py3.5.egg,\
$dependencies/geoip2-2.5.0-py3.5.egg,$dependencies/maxminddb-1.3.0-py3.5.egg,\
$dependencies/pytz-2017.2-py3.5.egg,$dependencies/dateutils-0.6.6-py3.5.egg \
--files GeoLite2/GeoLite2-ASN.mmdb,GeoLite2/GeoLite2-City.mmdb,GeoLite2/GeoLite2-Country.mmdb \
--master $master $arg_app
