# testinsert

This repository includes a java program that can be used to execute simple stress tests on INSERT EXPEDITED batches in parallel. 

You can choose how many competing threads will open connections and insert data into a stream concurrently.

## Building

Ensure that you have a SQLstream installed - the scripts rely on the `SQLSTREAM_HOME` variable being set. You can source the `/etc/sqlstream/environment` script to accomplish that.

## Installing

The install script sets up a stream and a couple of views in the `"testinsert"` schema:

```
    sqllineClient --run=teststream.sql
```

## Running

To view the program arguments:

```
    testinsert.sh -h
```

Start a sqlline session in a second terminal:

```
    sqllineClient
    SQL> select stream * from "testinsert"."monitorview";
```

Now start the java process:

```
    testinsert.sh -b 1000 -c 1000 -t 3 -w 0
```

Using a wait period of zero increases the load on the server.
