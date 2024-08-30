# Flink Dynamic Scaling

Enable scaling in Apache Flink dynamically without redeploying the streaming job.

This repository was developed from https://github.com/apache/flink/tree/release-1.14 and modified to enable dynamic scaling. Please refer to that URL for compilation.

### Features

* Trigger rescaling by invoking Flink's REST API:
  ```
  curl --location --request PATCH "http://job_manager_host:8081/jobs/{job_id}/rescaling?vertex={vertex_id}&parallelism={desired_parallelism}"
  ```
  You can get vertex_id by inspecting the HTML elements of the job topology in the Flink web dashboard
* Nexmark modification in https://github.com/takdir-rex/nexmark-windowed can be used for evaluation.
  Change the queries in https://github.com/takdir-rex/nexmark-windowed/tree/master/nexmark-flink/src/main/resources/queries as required.
  This dynamic scaling was evaluated using some selected original versions of Nexmark queries as in https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries
* A Just-in-Time (JIT) Snapshot is taken in each scaling event.
* Automation scripts are available at https://github.com/takdir-rex/flink-dynamic-scaling/tree/main/scripts
