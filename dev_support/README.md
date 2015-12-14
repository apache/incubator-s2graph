# Run S2Graph using Docker

1. Build a docker image of the s2graph in the project's root directory
	- `sbt "project s2rest_play" docker:publishLocal`
2. Run MySQL and HBase container first.
	- change directory to dev-support. `cd dev-support`
	- `docker-compose up -d graph_mysql` will run MySQL and HBase at same time.
3. Run graph container
	- `docker-compose up -d`

> S2Graph should be connected with MySQL at initial state. Therefore you have to run MySQL and HBase before running it.

## For OS X

In OS X, the docker container is running on VirtualBox. In order to connect with HBase in the docker container from your local machine. You have to register the IP of the docker-machine into the `/etc/hosts` file.

Within the `docker-compose.yml` file, I had supposed the name of docker-machine as `default`. So, in the `/etc/hosts` file, register the docker-machine name as `default`.

```
ex)
192.168.99.100 default
```

# Run S2Graph on your local machine

In order to develop and test S2Graph. You might be want to run S2Graph as `dev` mode on your local machine. In this case, the following commands are helpful.

- Run only MySQL and HBase

```
# docker-compose up -d graph_mysql
```

- Run s2graph as 'dev' mode

```
# sbt "project s2rest_play" run -Dhost=default
```

- or run test cases

```
# sbt test -Dhost=default
```
