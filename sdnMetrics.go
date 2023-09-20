package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const MetricsConfigFile = "./metrics.yaml"

var grafanaConfig GrafanaConfig

type DataSource struct {
	ID   int    `json:"id"`
	UID  string `json:"uid"`
	Name string `json:"name"`
}

type ResultsRaw struct {
	Results struct {
		A struct {
			Frames []ResultFrame `json:"frames"`
		} `json:"A"`
	} `json:"results"`
}

type ResultFrame struct {
	Schema ResultSchema `json:"schema"`
	Data   ResultData   `json:"data"`
}

type ResultData struct {
	Values [][]float64 `json:"values"`
}

type ResultSchema struct {
	Fields []ResultField `json:"fields"`
}

type ResultField struct {
	Name   string      `json:"name"`
	Type   string      `json:"type"`
	Labels ResultLabel `json:"labels"`
}

type ResultLabel struct {
	AgentId  string `json:"agent_id"`
	Mzone    string `json:"agent_tag_mzone_name"`
	HostName string `json:"host_hostname"`
	Le       string `json:"le"`
	TepIp    string `json:"tepIp"`
	Worker   string `json:"worker"`
}

type RequestData struct {
	Queries   []RequestQuery `json:"queries"`
	StartTime string         `json:"from"`
	EndTime   string         `json:"to"`
}

type RequestQuery struct {
	Expr         string `json:"expr"`
	DatasourceId int    `json:"datasourceId"`
}

func main() {
	metricsConfig := LoadMetricsConfig(MetricsConfigFile)
	grafanaConfig = metricsConfig.Grafana

	filePath := "./sdn_metrics_" + time.Now().Format("20060102150405") + ".xlsx"
	for index, metric := range metricsConfig.Metrics {
		if !metric.Enabled {
			continue
		}

		startTS, _ := time.Parse("2006-01-02 15:04:05", metric.TimeRange.Start)
		startTss := strconv.FormatInt(startTS.UnixMilli(), 10)
		endTS, _ := time.Parse("2006-01-02 15:04:05", metric.TimeRange.End)
		endTss := strconv.FormatInt(endTS.UnixMilli(), 10)

		fmt.Println("-----------------------------------------------")
		fmt.Printf("Starting to fetch metrics [%s] from [%s] ...\n", metric.Metric, metric.Region)
		dataSource, err := getDataSource(metric.Region)
		if err != nil {
			fmt.Println("Failed to get datasource of the region: ", metric.Region)
			continue
		}
		start := time.Now()
		results := query(dataSource.ID, metric.Metric, metric.Labels, startTss, endTss)
		fmt.Printf("Fetching metrics [%s] from [%s] is finished. (%s)\n",
			metric.Metric, metric.Region, time.Since(start).String())
		resultData := results.Results.A.Frames
		if len(resultData) > 0 && len(resultData[0].Data.Values) > 0 {
			fmt.Println("Starting to aggregate metrics data and save to the local file:", filePath)
			exportMetricsHist(resultData, filePath, metric.Metric[20:]+"_"+strconv.Itoa(index+1))
			fmt.Printf("The metrics [%s] from [%s] is saved to: %s\n", metric.Metric, metric.Region, filePath)
		}
	}
	fmt.Println("SDN metrics query is finished!")
}

// getDataSource - get datasource by region
func getDataSource(region string) (DataSource, error) {
	client := &http.Client{}
	url := "http://" + grafanaConfig.Server + ":" + grafanaConfig.Port + "/api/datasources/name/" + region
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("Error creating HTTP request:", err)
	}
	req.Header.Add("Authorization", grafanaConfig.ApiKey)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing body:", err)
		}
	}(resp.Body)
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	var dataSource DataSource
	err = json.Unmarshal(bodyBytes, &dataSource)
	if err != nil {
		return DataSource{}, err
	}

	return dataSource, nil
}

// query - Query the metrics data from Grafana/Sysdig
func query(dataSourceId int, metric string, labels []string, startTime string, endTime string) ResultsRaw {
	var results ResultsRaw
	exprs := metric + "{"
	for _, label := range labels {
		exprs += label + ","
	}
	exprs = strings.TrimRight(exprs, ",") + "}"

	rq := RequestQuery{exprs, dataSourceId}
	queries := []RequestQuery{rq}
	requestData := RequestData{queries, startTime, endTime}
	jsonReq, err := json.Marshal(requestData)

	client := &http.Client{}
	url := "http://" + grafanaConfig.Server + ":" + grafanaConfig.Port + "/api/ds/query"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonReq))
	if err != nil {
		fmt.Println("Error creating HTTP request:", err)
		return results
	}
	req.Header.Add("Authorization", grafanaConfig.ApiKey)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending HTTP request:", err)
		return results
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Failed to close body", err)
		}
	}(resp.Body)

	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(bodyBytes, &results)
	if err != nil {
		fmt.Println("Failed to unmarshal json data", err)
	}

	return results
}

type AggValue struct {
	Mzone string
	Le    string
	Value float64
}

type AggValueSort []*AggValue

func (avs AggValueSort) Len() int { return len(avs) }

func (avs AggValueSort) Swap(i, j int) { avs[i], avs[j] = avs[j], avs[i] }

func (avs AggValueSort) Less(i, j int) bool {
	if avs[i].Mzone < avs[j].Mzone {
		return true
	}

	if avs[i].Mzone == avs[j].Mzone {
		if avs[i].Le == "+Inf" {
			return false
		}

		if avs[j].Le == "+Inf" {
			return true
		}

		lei, _ := strconv.ParseFloat(avs[i].Le, 2)
		lej, _ := strconv.ParseFloat(avs[j].Le, 2)
		if lei < lej {
			return true
		} else {
			return false
		}
	}
	return false
}

// export - Export metrics data to a local Excel
func exportMetricsHist(metricsData []ResultFrame, filePath string, sheetName string) {
	var aggValues []*AggValue
	allHosts := make(map[string][]string)
	// hosts of invalid metrics data
	invalidHosts := make(map[string][]string)
	for _, mtdata := range metricsData {
		var diffValue float64
		count := len(mtdata.Data.Values[1])
		if count == 0 {
			return
		}

		hosts, ok := allHosts[mtdata.Schema.Fields[1].Labels.Mzone]
		if !ok {
			hosts = make([]string, 1)
		}
		if !search(mtdata.Schema.Fields[1].Labels.HostName, hosts) {
			allHosts[mtdata.Schema.Fields[1].Labels.Mzone] = append(hosts, mtdata.Schema.Fields[1].Labels.HostName)
		}

		// handle for accumulated type
		if count > 1 {
			if mtdata.Data.Values[1][0] > mtdata.Data.Values[1][count-1] {
				hosts, ok := invalidHosts[mtdata.Schema.Fields[1].Labels.Mzone]
				if !ok {
					hosts = make([]string, 1)
				}
				if !search(mtdata.Schema.Fields[1].Labels.HostName, hosts) {
					invalidHosts[mtdata.Schema.Fields[1].Labels.Mzone] = append(hosts, mtdata.Schema.Fields[1].Labels.HostName)
				}
				continue
			}
			diffValue = mtdata.Data.Values[1][count-1] - mtdata.Data.Values[1][0]
		} else {
			diffValue = mtdata.Data.Values[1][0]
		}

		isExistent := false
		for _, v := range aggValues {
			if v.Mzone == mtdata.Schema.Fields[1].Labels.Mzone && v.Le == mtdata.Schema.Fields[1].Labels.Le {
				v.Value += diffValue
				isExistent = true
				break
			}
		}
		if !isExistent {
			aggValues = append(aggValues, &AggValue{mtdata.Schema.Fields[1].Labels.Mzone,
				mtdata.Schema.Fields[1].Labels.Le, diffValue})
		}
	}

	for mzone, hosts := range allHosts {
		fmt.Printf("The total host count in '%s': %d\n", mzone, len(hosts))
	}

	for mzone, hosts := range invalidHosts {
		fmt.Printf("The count of hosts with invalid metrics in '%s': %d\n", mzone, len(hosts))
	}

	var bucketAggValues []*AggValue
	sort.Sort(AggValueSort(aggValues))
	if len(aggValues) == 0 {
		return
	}
	leastLe := aggValues[0].Le
	bucketAggValues = append(bucketAggValues, aggValues[0])
	for i := 1; i < len(aggValues); i++ {
		if aggValues[i].Le == leastLe {
			bucketAggValues = append(bucketAggValues, aggValues[i])
		} else {
			bucketAggValues = append(bucketAggValues,
				&AggValue{aggValues[i].Mzone, aggValues[i].Le, aggValues[i].Value - aggValues[i-1].Value})
		}
	}

	// Open or Create Excel file
	_, file := openExcelFile(filePath, sheetName)
	file.SetCellValue(sheetName, "A1", "MZONE")
	file.SetCellValue(sheetName, "B1", "LE")
	file.SetCellValue(sheetName, "C1", "Metrics")
	for index, avs := range bucketAggValues {
		file.SetCellValue(sheetName, fmt.Sprintf("A%d", index+2), avs.Mzone)
		file.SetCellValue(sheetName, fmt.Sprintf("B%d", index+2), avs.Le)
		file.SetCellFloat(sheetName, fmt.Sprintf("C%d", index+2), avs.Value, 0, 64)
	}

	// insert column chart based on metrics data
	insertColChart(file, sheetName, len(aggValues)+1)
	if err := file.SaveAs(filePath); err != nil {
		fmt.Println(err)
	}
}

// export - Export specific time point of metrics data to a local Excel
func exportSpecificTimeHist(metricsData []ResultFrame, filePath string, sheetName string) {
	var aggValues []*AggValue
	for _, mtdata := range metricsData {
		var diffValue float64
		count := len(mtdata.Data.Values[1])
		if count == 0 {
			return
		}

		isExistent := false
		for _, v := range aggValues {
			if v.Mzone == mtdata.Schema.Fields[1].Labels.Mzone && v.Le == mtdata.Schema.Fields[1].Labels.Le {
				v.Value += mtdata.Data.Values[1][0]
				isExistent = true
				break
			}
		}
		if !isExistent {
			aggValues = append(aggValues, &AggValue{mtdata.Schema.Fields[1].Labels.Mzone,
				mtdata.Schema.Fields[1].Labels.Le, diffValue})
		}
	}

	var bucketAggValues []*AggValue
	sort.Sort(AggValueSort(aggValues))
	if len(aggValues) == 0 {
		return
	}
	leastLe := aggValues[0].Le
	bucketAggValues = append(bucketAggValues, aggValues[0])
	for i := 1; i < len(aggValues); i++ {
		if aggValues[i].Le == leastLe {
			bucketAggValues = append(bucketAggValues, aggValues[i])
		} else {
			bucketAggValues = append(bucketAggValues,
				&AggValue{aggValues[i].Mzone, aggValues[i].Le, aggValues[i].Value - aggValues[i-1].Value})
		}
	}

	// Open or Create Excel file
	_, file := openExcelFile(filePath, sheetName)
	file.SetCellValue(sheetName, "A1", "MZONE")
	file.SetCellValue(sheetName, "B1", "LE")
	file.SetCellValue(sheetName, "C1", "Metrics")
	for index, avs := range bucketAggValues {
		file.SetCellValue(sheetName, fmt.Sprintf("A%d", index+2), avs.Mzone)
		file.SetCellValue(sheetName, fmt.Sprintf("B%d", index+2), avs.Le)
		file.SetCellFloat(sheetName, fmt.Sprintf("C%d", index+2), avs.Value, 0, 64)
	}

	// insert column chart based on metrics data
	insertColChart(file, sheetName, len(aggValues)+1)
	if err := file.SaveAs(filePath); err != nil {
		fmt.Println(err)
	}
}

// openExcelFile - Open or Create Excel file
func openExcelFile(filePath string, sheetName string) (error, *excelize.File) {
	// Open or Create Excel file
	file, err := excelize.OpenFile(filePath)
	if err != nil {
		file = excelize.NewFile()
		err := file.SetSheetName("Sheet1", sheetName)
		if err != nil {
			fmt.Println(err)
			return err, nil
		}
	} else {
		_, err := file.NewSheet(sheetName)
		if err != nil {
			fmt.Println(err)
			return err, nil
		}
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	return err, file
}

// insertChart - insert column chart based on the metrics data
func insertColChart(file *excelize.File, sheetName string, position int) {
	if err := file.AddChart(sheetName, "E1", &excelize.Chart{
		Type: excelize.Col,
		Series: []excelize.ChartSeries{
			{
				// Name:       sheetName + "!$A$2:$A$" + strconv.Itoa(position),
				Categories: sheetName + "!$A$2:$B$" + strconv.Itoa(position),
				Values:     sheetName + "!$C$2:$C$" + strconv.Itoa(position),
			},
		},
		Format: excelize.GraphicOptions{
			OffsetX: 15,
			OffsetY: 10,
		},
		Legend: excelize.ChartLegend{
			Position: "none",
		},
		Title: []excelize.RichTextRun{
			{
				Text: sheetName,
			},
		},
		PlotArea: excelize.ChartPlotArea{
			ShowCatName:     false,
			ShowLeaderLines: false,
			ShowPercent:     true,
			ShowSerName:     false,
			ShowVal:         false,
		},
	}); err != nil {
		fmt.Println(err)
		return
	}
}

type MetricsConfig struct {
	Grafana GrafanaConfig   `yaml:"grafana"`
	Metrics []RequestMetric `yaml:"metrics"`
}

type GrafanaConfig struct {
	Server string `yaml:"server"`
	Port   string `yaml:"port"`
	ApiKey string `yaml:"api_key"`
}

type RequestMetric struct {
	Metric    string           `yaml:"metric"`
	Region    string           `yaml:"region"`
	Mzone     string           `yaml:"mzone"`
	Labels    []string         `yaml:"labels"`
	TimeRange RequestTimeRange `yaml:"time_range"`
	Enabled   bool             `yaml:"enabled"`
}

type RequestTimeRange struct {
	Start string `yaml:"start"`
	End   string `yaml:"end"`
}

// LoadMetricsConfig - load metrics configuration and input
func LoadMetricsConfig(configFile string) MetricsConfig {
	var metricsConfig MetricsConfig
	yamlFile, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = yaml.Unmarshal(yamlFile, &metricsConfig)
	if err != nil {
		fmt.Println(err.Error())
	}
	return metricsConfig
}

// search - check if the array contains the specific string
func search(target string, strArray []string) bool {
	if len(strArray) == 0 {
		return false
	}
	sort.Strings(strArray)
	index := sort.SearchStrings(strArray, target)
	if index < len(strArray) && strArray[index] == target {
		return true
	}
	return false
}
