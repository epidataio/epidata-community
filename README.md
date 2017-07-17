Epidata
=====================================
EpiData is an integrated IoT Analytics Platform. It enables smarter industrial solutions including energy management, building automation and smart manufacturing. 


Required Components
--------------------
Apache Cassandra  
Apache Kafka  
Apache Spark  
Jupyter Notebook  


Installation and Launch
------------------------
Detailed installation and launch scripts are available in epidata-install repository.


Configuration
--------------
- Authentication: 
Access to EpiData platform is primarily managed via OAuth 2 authorization. The configuration settings for OAuth 2 are available in play/conf/securesocial.conf. Authenticated users can be added to the system manually via Cassandra's CQL commands.

- Measurement Class:
Epidata can be configured to operate on 'sensor measurement' or 'automated test' data. To enable sensor measurement data, play's application.conf must set measurement-class to sensor_measurement. In addition, spark's spark-defaults.conf must set spark.epidata.measurementClass to sensor_measurement. To enable automated test data, play's application.conf must set measurement-class to automated_test. In addition, spark's spark-defaults.conf must set spark.epidata.measurementClass to automated_test. 


Usage
------
After launch, EpiData platform is ready to ingest and process sensor data. To access ingestion scripts and Jupyter Notebook tutorial, you can visit https://<epidata_url>, where epidata_url is the url for the server hosting EpiData.


Resources
----------
EpiData community-edition is supported via its GitHub repository site.