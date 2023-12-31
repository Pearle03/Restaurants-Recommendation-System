## Steps to run Static Part
Enter command code: scala StaticRecommendation.scala 

Program will ask for command line input:

Enter type of Food: Indian
Enter City: Pittsburgh
Enter State: PA
Enter User ID: Fr12lvqUHN6dmMysQ

Based on your search, the top 5 Restaurants will be displayed.

## Steps to run Dynamic streaming
1. Start the following servers </br>
	a) Zookeeper: **bin/zookeeper-server-start.sh config/zookeeper.properties** </br>
	b) Kafka: **bin/kafka-server-start.sh config/server.properties** </br>
	c) Elastic: **bin/elasticsearch** </br>
	d) Kibana: **bin/kibana** </br>
2. Run PySpark using command: pyspark
3. Run the Kafka Producer(yelpScrapper.py) in PySpark
4. Run the Kafka Consumer(consumer.py) in PySpark
5. Run the Recommender program (recommender.py) in PySpark
6. Open the kibana server in local browser(localhost:5601) for the visualization.