# SDNMetrics

## Introduction
SDNMetrics is a program script that is to fetch SDN metrics data as per metrics input request such as `Region`, `Mzone`, 
specific `hosts`, `time range` or all any other supported labels for the specific metrics, and export the metrics data to a local `Excel` format file 
with column chart included. By using the yaml format of modular and readable input file metrics.yaml, 
you can easily to set for querying multiple same/different metrics at the same time and export all the metrics data to a single file.

## Background
We have implemented SDN firewall connection metrics data collection and verified the metrics data are correctly in local dev mzone. 
These connection metrics data are sending to Sysdig at the same time, we need to verify firewall connection metrics data to make sure they are accurate in Sysdig as well.   
One practical way is using Grafana with Prometheus plugin to set up all metrics dashboard for each region, but as there are thousands of hosts with each Sysdig region, 
it is hard to verify the metrics data through Grafana dashboard, and for some special use case of handling metrics data for specific hosts, 
which cannot be achieved through Grafana/Prometheus, this is reason why we need this program script by which to call Grafana APIs 
to fetch the metrics and export to a local file like the attached file for data verification and further analysis.

## Features
- Support multiple same/different metrics query at the same time and all metrics data are exported to a single visual Excel format file with column chart included.
- Filter metrics data by multiple labels based on metrics, such as `region`, `mzone`, `hosts`, `time range` or any other supported labels based on metrics.
- Easily enable/disable the specific metrics though the readable input yaml file.
- Supports for not limited to the following metrics:
    - **`VnicNewTcpConnRateHist`**: 
Prometheus histogram that consists of new Tcp connection rate (connections per second) observed on each of the vnics 
    - **`VnicNewUdpConnRateHist`**:
Prometheus histogram that consists of new Udp connection rate (connections per second) observed on each of the vnics
    - **`TcpConnDurationHist`**:
Prometheus histogram that consists of duration time (seconds) observed on each of Tcp connections on the node
program scrip can be easies to extended and enhanced.


## Configuration
### Step 1. Setup Grafana and Sysdig datasource
1. Install Grafana [https://grafana.com/docs/grafana/latest/setup-grafana/installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)
2. Check Sysdig API token through IBM Cloud Sysdig dashboard.
3. Configure prometheus datasource to connect to region Sysdig through Grafana interface. 
Add a new header `authorization` with Sysdig API token appending the prefix `Bearer{space}` as value.

### Step 2. Generate Grafana API key
1. Issue the following command on grafana host:
```bash
curl -X POST -H "Content-Type: application/json" -d '{"name":"apikeycurl", "role": "Admin"}' http://{user}:{password}@localhost:3000/api/auth/keys
```
2. Update the API key with `Bearer{space}` prefix into the section `grafana` in metrics.yaml

### Step3. Input metrics to query
Update `metrics.yaml` with your metrics to query in the section `metrics`
![metrics.png](files/metrics.png)

## Execution

### build
```bash
go build sdnMetrics.go
```
### run
```bash
./sdnMetrics
```

### Output 

#### Console output (example)
```
-----------------------------------------------
Starting to fetch metrics [fabcon_iob_fw_stats_TcpConnDurationHist_bucket] from [us-south-prod] ...
Fetching metrics [fabcon_iob_fw_stats_TcpConnDurationHist_bucket] from [us-south-prod] is finished. (52.603651666s)
Starting to aggregate metrics data and save to the local file: ./sdn_metrics_20230920152026.xlsx
The total host count in 'mzone717': 1492
The total host count in 'mzone737': 664
The total host count in 'mzone727': 1238
The count of hosts with invalid metrics in 'mzone717': 124
The count of hosts with invalid metrics in 'mzone737': 63
The count of hosts with invalid metrics in 'mzone727': 82
The metrics [fabcon_iob_fw_stats_TcpConnDurationHist_bucket] from [us-south-prod] is saved to: ./sdn_metrics_20230920152026.xlsx
-----------------------------------------------
Starting to fetch metrics [fabcon_iob_fw_stats_TcpConnDurationHist_bucket] from [us-south-prod] ...
Fetching metrics [fabcon_iob_fw_stats_TcpConnDurationHist_bucket] from [us-south-prod] is finished. (16.54616875s)
Starting to aggregate metrics data and save to the local file: ./sdn_metrics_20230920152026.xlsx
The total host count in 'mzone717': 1492
The count of hosts with invalid metrics in 'mzone717': 124
The metrics [fabcon_iob_fw_stats_TcpConnDurationHist_bucket] from [us-south-prod] is saved to: ./sdn_metrics_20230920152026.xlsx
-----------------------------------------------
Starting to fetch metrics [fabcon_iob_fw_stats_VnicNewTcpConnRateHist_bucket] from [us-south-prod] ...
Fetching metrics [fabcon_iob_fw_stats_VnicNewTcpConnRateHist_bucket] from [us-south-prod] is finished. (12.599186042s)
Starting to aggregate metrics data and save to the local file: ./sdn_metrics_20230920152026.xlsx
The total host count in 'mzone717': 1492
The total host count in 'mzone737': 664
The total host count in 'mzone727': 1238
The metrics [fabcon_iob_fw_stats_VnicNewTcpConnRateHist_bucket] from [us-south-prod] is saved to: ./sdn_metrics_20230920152026.xlsx
-----------------------------------------------
Starting to fetch metrics [fabcon_iob_fw_stats_VnicNewUdpConnRateHist_bucket] from [us-south-prod] ...
Fetching metrics [fabcon_iob_fw_stats_VnicNewUdpConnRateHist_bucket] from [us-south-prod] is finished. (18.125788916s)
Starting to aggregate metrics data and save to the local file: ./sdn_metrics_20230920152026.xlsx
The total host count in 'mzone717': 1492
The total host count in 'mzone737': 664
The total host count in 'mzone727': 1238
The metrics [fabcon_iob_fw_stats_VnicNewUdpConnRateHist_bucket] from [us-south-prod] is saved to: ./sdn_metrics_20230920152026.xlsx
SDN metrics query is finished!
```

#### File output (example)
![metrics_output.png](files/metrics_output.png)
![sdn_metrics.xlsx](files/sdn_metrics.xlsx)

## Special Case
For metrics `fabcon_iob_fw_stats_TcpConnDurationHist_bucket`, as some nodes/hosts may be restarted during the time range, 
which may causes the later metric data could be less than the previous metric data, 
that results in negative value in these host/worker aggregation, the program script will ignore these invalid metrics data from the host/workers.

## 3rd-Party Library
 [https://github.com/xuri/excelize](https://github.com/xuri/excelize)