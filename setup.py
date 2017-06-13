from setuptools import setup, find_packages

setup(
    name = "sflow_analysis",
    version = "1.0.0",
    packages = find_packages(),
    author='BWSW',
    install_requires=[
    "geoip2==2.5.0",
    "influxdb==4.0.0",
    "kafka==1.3.3",
    "nanotime==0.5.2"
   ],
)

