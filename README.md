# Spark batch library framework: CSV 2 MongoDB

Spark based applications with the capability to load millions of
records in a CSV file to a MongoDB. Nevertheless, the library is 
generic enough to accommodate other sources, as well as other targets.

## Configuring
### Reader config
Under `src/main/resources/reader_config` you will find a json file, which has
the following naming convention: `<source_name>.json`. The `source_name`
is user-defined, and it is just a way to label logically source data. In
this case, I'm loading a movies dataset, with metadata, so I decided to
label it as `movies_metadata`. The structure of the config file is:
```json
{
  "sparkOptions": {
    ...
  },
  "jsonFields": {
    ...
  }
}
```
In `sparkOptions` one needs to provide the options available to a spark
reader, with the actual keys expected by the reader and the values.
In `jsonFields` one lists the fields from the incoming dataset that are
stringified json fields. If you need to further extend these settings, feel
free to do so and update the code accordingly.

### Schemas
It is possible to define two types of schemas, schema on read time and
schema on parse time. Each type of schema can be declared under
`src/main/resources/schemas/parser` and `src/main/resources/schemas/parser`,
respectively.

The convention for schema definition is avro. In case reader and parser
schemas match, you still __MUST__ provide both schemas twice, once under one each
directory.

## Usage
### Requirements
* Git
* Java 8
* Spark 2.4.6
* Scala 2.11.12

### Building
```shell
> git clone https://github.com/jsoft88/spark-csv2mongodb.git
> cd spark-csv2mongodb
> sbt assembly
```
After sbt completes, you will find the shippable jar file under
`target/scala-2.11/batch-job-lib-assembly-0.1.jar`.

### Execution
```shell
spark-submit --master <spark://spark-master:7077> \
  --class com.org.batch.Main \
  /path/to/the/jar/batch-job-lib-assembly-0.1.jar \
  --reader-type csv \
  --writer-type mongodb \
  --transformation-type no-op \
  --input-source file:///home/bitnami/data/movies_metadata.csv \
  --mongo-output-uri mongodb://db:27017 \
  --app-name csv2mongo \
  --reader-config-key movies_metadata \
  --mongo-output-database movies \
  --mongo-output-collection movies_metadata
```

* --reader-type: currently, only `csv` is supported.
* --writer-type: currently, only `mongodb` is supported.
* --transformation-type: currently, only `no-op` is supported
* --input-source-file: absolute path (with protocol) expected.
This should be a shared directory, otherwise distributed mode of spark
  will fail.
* --mongo-output-uri: mongo db uri
* --app-name: a name for the application
* --reader-config-key: the logical name one gives to the input data. This
will be used to retrieve the configuration file under `resources/reader_config`.
* --mongo-output-database: name of the database containing the collection to which
the csv file will be dumped
* --mongo-output-collection: name of the collection that will contain documents
from csv file.
* --mongo-username-env-key: name of the environment variable exported
containing the username to use when connecting to mongodb
* --mongo-password-env-key: name of the environment variable exported
containing the password to use for the provided mongodb user
  
## CICD
You can find integration with github actions, which is in charged of running
integration tests for the batch library. This also serves as an example of how
to run locally by using docker-compose. The root username and root password
used in the docker setup, can be found in the `Settings` tab of the repo.

## Extending the batch library
Please feel free to fork and extend the library for other sources/targets,
eventually you could use this library to populate SQL/NoSQL databases. Also,
feasible for other types of batch jobs. In combination with the streaming lib,
in my repo, you can have a leaner tech stack for real-time/batch jobs.