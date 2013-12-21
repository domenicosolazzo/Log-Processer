Log-Processer
=============

Storm Log Processing

Implementation recipe for an enterprise log storage and a search 
and analysis solution based on the Storm processor.

### INSTALLATION

* Download and configure logstash
  - wget https://logstash.objects.dreamhost.com/release/logstash-1.1.7-monolithic.jar
* Start logstash
  - java –jar logstash-1.1.7-monolithic.jar –f shipper.conf
