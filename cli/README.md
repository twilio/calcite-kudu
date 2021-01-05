# SQL Client Example

This directory includes a [Docker Compose](https://docs.docker.com/compose/) configuration which makes it easy to play around with this adapter. To run it install [Docker Compose by following the instructions](https://docs.docker.com/compose/install/). With the Docker Daemon started, you can run

``` bash
# Go back to the root of this repository
cd ../
# package up the cli
mvn package -pl cli -am -DskipTests

# From within *this directory*. It will use the docker-compose.yml file in this directory
# and the Dockerfile in this directoyr
docker-compose -f cli/docker-compose.yml up --build -d
```
This will setup the Kudu daemons and a container connected to the network. This last container can be used to access a sql shell by running the following command

``` bash
docker-compose -f cli/docker-compose.yml exec command-line java -jar kudu-client.jar -c kudu-master-1,kudu-master-2,kudu-master-3
```
This uses the `kudu-client.jar` to connect to the three master hosts and opens the interactive shell. In this directory there is a set of [example SQL commands](./example.sql) that can be entered into the CLI opened above.
