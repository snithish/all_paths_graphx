# B: Spark

Task is accomplished using [Scala 2.12.12](https://www.scala-lang.org/api/2.12.12/)
with [sbt](https://www.scala-sbt.org/) used as build tool.

Core logic is encapsulated in [TaskOne.scala](src/main/scala/com/snithish/TaskOne.scala)
and [TaskTwo.scala](src/main/scala/com/snithish/TaskTwo.scala)
with [App.scala](src/main/scala/com/snithish/App.scala) acting as driver and main entry point.

# Usage:

## Prerequisites:

1. Java 8
2. Scala 2.12.12

## Local:

### Task One:

```shell
./sbtx "run taskOne /input-path/data/customers.csv /input-path/data/orders.csv /output-path/data/taskOne.csv"
```

### Task Two:

```shell
./sbtx "run taskTwo taskTwo 17274 /input-path/data/CA-GrQc.txt /output-path/data/taskTwo.csv"
```

## Docker

### Task One:

```shell
docker run -it --rm -v $(pwd):/app --workdir "/app" --name task_b hseeberger/scala-sbt:11.0.9.1_1.4.6_2.12.12 sbt "run taskOne /app/data/customers.csv /app/data/orders.csv /app/data/taskOne.csv"
```

### Task Two:

```shell
docker run -it --rm -v $(pwd):/app --workdir "/app" --name task_b hseeberger/scala-sbt:11.0.9.1_1.4.6_2.12.12 sbt "run taskTwo 17274 /app/data/CA-GrQc.txt /app/data/taskTwo.csv"
```

Results will be written to [data](data) folder.
