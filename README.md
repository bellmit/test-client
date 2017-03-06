modified mqtt broker
=================

modified mqtt broker, removing all undesired features, based on [vertx-mqtt-broker](https://github.com/GruppoFilippetti/vertx-mqtt-broker)

Quick Start
-----------
Requires Vert.x 3.1.x and Maven 3+

```
cd mqtt-broker
mvn clean install
```

run as normal java ...
```
java -jar target/modified-mqtt-broker-<version>-fat.jar -c demo.json
```

