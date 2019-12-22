# CS 410 Project
## Badrul Chowdhury (bchow8@illinois.edu)

## Prerequisites
1. Project proposal

Please refer to the project proposal (attached) before proceeding as it will set the groundwork for the implementation details in this document.

2. Docker

Docker Desktop (DD) is required to run the ELK stack for this demo. You can download DD for your platform from [the official Docker site](https://docs.docker.com/).


## Overview
The goal of this project is to take the first steps in integrating Elasticsearch (ES) with Apache Drill (Drill).

### Objective 
Add the capability for Drill to query ES using Drill's command line (sqlline).

### Goals
Read string data from an arbitrary index (table) in ES from Drill.

### Stretch Goal
Read numeric data (integer + floats) from ES index.

## Design Choices
We can develop extensions for Drill via the following:
1. Custom data format
2. Storage plugin

TODO

## Code changes
You can find all diffs with the master branch at `./master.diff`. This contains way too many changes, so we included a smaller subset of relevant changes at `./relevant_master.diff`.

The entire code takes ~15m to build, so we have included the binaries along with the sources to be able to run the demo fast.

## Demo Steps
### Run the provided script
Run this: `chmod +x ./run_demo.sh && ./run_demo.sh`

The script does the following for you:

1. Kill all existing docker containers:
```
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)
```

2. Start ELK containers. Elasticsearch for the demo, Kibana for debugging, and Logstash for fun.
```
docker-compose up -d
```

- You can check the status of the containers using `docker ps`.

3. Install the Drill binaries and start sqlline by invoking `./start-drill.sh`

You should now see a prompt like this: 
`apache drill>`. This means sqlline is up and running.

### Run the demo query at sqlline

1. Get the location of the test file `test.elasticsearch` on your system by running `pwd`.

2. Now you can run the following at the prompt:
```
apache drill> select columns from dfs.`<output of pwd>/test.elasticsearch`;
```

3. You should see the output of `test.elasticsearch` at the sqlline prompt:
```
apache drill> select columns from dfs.`/Users/badrulchowdhury/code/drill/test.elasticsearch`;
+-------------------------------------+
|               columns               |
+-------------------------------------+
| ["localhost:9200:elastic:changeme"] |
+-------------------------------------+
1 row selected (0.152 seconds)
```

4. This demonstrates that Drill was able to connect to ES. You can press `!q` to exit sqlline.

## Thoughts on the Demo
We met the goal and partially met the stretch goal.

## Future Work
1. Integrate sqlline to output the result of running the query.
2. Support for querying nested documents in ES.
3. Support for all data types.
4. Join ES data with data from other sources.
5. Performance considerations.

## References
1. https://www.amazon.com/Learning-Apache-Drill-Analyze-Distributed/dp/1492032794
2. https://www.amazon.com/Mastering-Elasticsearch-Second-Rafal-Kuc/dp/1783553790

