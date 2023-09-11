# B: Spark

Task is accomplished using [Scala 2.12.12](https://www.scala-lang.org/api/2.12.12/)
with [sbt](https://www.scala-sbt.org/) used as build tool.

# Usage:

## Prerequisites:

1. Java 8
2. Scala 2.12.12

## Local:

```shell
./sbtx "run"
```

## Docker

```shell
docker run -it --rm -v $(pwd):/app --workdir "/app" --name task_b hseeberger/scala-sbt:11.0.9.1_1.4.6_2.12.12 sbt "run"
```