package main

import (
	"bufio"
	"context"
	"crypto"
	_ "embed"
	"encoding/json"
	"fmt"
	daterange "github.com/felixenescu/date-range"
	"github.com/mitchellh/mapstructure"
	"github.com/mixpanel/mixpanel-go"
	"os"
	"strings"
	"time"
)

//go:embed credentials.schema.json
var credentialSchemaString string
var credentialSchema = UnmarshalSchema(credentialSchemaString)

//go:embed row.schema.json
var rowSchemaString string
var rowSchema = UnmarshalSchema(rowSchemaString)

type Message struct {
	Type      string `json:"type"`
	Direction string `json:"direction"`
	Payload   any    `json:"payload"`
}

type RowPayload struct {
	Date         string  `mapstructure:"date"`
	Source       string  `mapstructure:"source"`
	CampaignId   any     `mapstructure:"campaign_id"`
	CampaignName string  `mapstructure:"campaign_name"`
	GroupId      any     `mapstructure:"group_id"`
	AdId         any     `mapstructure:"ad_id"`
	Cost         float64 `mapstructure:"cost"`
	Clicks       float64 `mapstructure:"clicks"`
	Impressions  float64 `mapstructure:"impressions"`
	Conversions  float64 `mapstructure:"conversions"`
	UtmSource    string  `mapstructure:"utm_source"`
	UtmCampaign  string  `mapstructure:"utm_campaign"`
	UtmMedium    string  `mapstructure:"utm_medium"`
	UtmTerm      string  `mapstructure:"utm_term"`
	UtmContent   string  `mapstructure:"utm_content"`
}

type Status struct {
	Received int `json:"received"`
	Success  int `json:"success"`
	Skipped  int `json:"skipped"`
	Failed   int `json:"failed"`
}

var lookbackWindow = 2
var initialSyncDays = 30
var batchSize = 2000
var syncId string
var stateKey []string

var rpcClient = NewRpcClient(os.Getenv("RPC_URL"))

var batch []*mixpanel.Event
var initialState daterange.DateRanges
var commitedState daterange.DateRanges
var processedRanges daterange.DateRanges
var startTime = time.Now()
var lastDate = startTime
var statuses = make(map[string]*Status)

var lastProcessedDate string
var currentStatus *Status

func main() {
	var mp *mixpanel.ApiClient
	//mp := mixpanel.NewApiClient("PROJECT_TOKEN")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var message Message
		err := json.Unmarshal([]byte(line), &message)
		if err != nil {
			lerror("Message received cannot be parsed: "+line, err.Error())
			os.Exit(1)
		}
		switch message.Type {
		case "describe":
			reply("spec", map[string]any{
				"roles":                 []string{"destination"},
				"description":           "Mixpanel Connector",
				"connectionCredentials": credentialSchema,
			})
			os.Exit(0)
		case "describe-streams":
			reply("stream-spec", map[string]any{
				"roles":         []string{"destination"},
				"defaultStream": "AdData",
				"streams":       []any{map[string]any{"name": "AdData", "rowType": rowSchema}},
			})
		case "start-stream":
			payload := message.Payload.(map[string]any)
			stream, ok := payload["stream"]
			if !ok || stream != "AdData" {
				lerror("Unknown stream", stream)
				reply("halt", map[string]any{
					"message": fmt.Sprintf("Unknown stream: %s", stream),
				})
				os.Exit(1)
			}
			syncId, _ = payload["syncId"].(string)
			creds, ok := payload["connectionCredentials"].(map[string]any)
			if !ok {
				lerror("No credentials provided: " + line)
				reply("halt", map[string]any{
					"message": "connectionCredentials are required",
				})
			}
			projectToken, _ := creds["projectToken"].(string)
			residency, _ := creds["residency"].(string)
			rInitialSyncDays, ok := creds["initialSyncDays"].(float64)
			if ok {
				initialSyncDays = int(rInitialSyncDays)
			}
			rLookbackWindow, ok := creds["lookbackWindow"].(float64)
			if ok {
				lookbackWindow = int(rLookbackWindow)
			}
			rBatchSize, ok := creds["batchSize"].(float64)
			if ok {
				batchSize = int(rBatchSize)
			}
			stateKey = []string{"syncId=" + syncId, "type=mixpanel.state"}
			raw, err := rpcClient.Get(stateKey)
			if err != nil {
				lerror("Error getting state", err.Error())
			} else {
				initialState, err = dateRangesFromAny(raw)
				if err != nil {
					lerror("Error parsing state", err.Error())
				} else if !initialState.IsZero() {
					processedRanges = daterange.NewDateRanges(initialState.ToSlice()...)
					commitedState = daterange.NewDateRanges(initialState.ToSlice()...)
					info("State loaded", fmt.Sprint(initialState))
					lastDate = initialState.LastDate()
				}
			}
			if residency == "EU" {
				mp = mixpanel.NewApiClient(projectToken, mixpanel.EuResidency())
			} else {
				mp = mixpanel.NewApiClient(projectToken)
			}
			info(fmt.Sprintf("Stream '%s' started. Residency: %s SyncId: %s InitialSyncDays: %d LookbackWindow: %d", stream, residency, syncId, initialSyncDays, lookbackWindow))
		case "end-stream":
			info("Received end-stream message.")
			sendBatch(mp)
			reply("stream-result", statuses)
			time.AfterFunc(1000, func() {
				info("Bye!")
				os.Exit(0)
			})
		case "row":
			payload := message.Payload.(map[string]any)
			var rowPayload RowPayload
			err = mapstructure.Decode(payload["row"], &rowPayload)
			if err != nil {
				lerror("Cannot parse row payload: "+line, err.Error())
				os.Exit(1)
			} else {
				processRow(mp, &rowPayload)
			}
		default:
			lerror("Unknown message type", message.Type)
		}
	}
	err := scanner.Err()
	if err != nil {
		logErr(err)
	}
}

func processRow(mp *mixpanel.ApiClient, payload *RowPayload) {
	if lastProcessedDate != payload.Date {
		if lastProcessedDate != "" {
			sendBatch(mp)
		}
		lastProcessedDate = payload.Date
		currentStatus = getStatus(payload.Date)
	}
	currentStatus.Received++
	t, err := time.Parse(time.DateOnly, payload.Date)
	if err != nil {
		currentStatus.Failed++
		lerror("Error parsing time: "+payload.Date, err.Error())
		return
	}
	initialSyncStart := startTime.Truncate(time.Hour * 24).Add(time.Hour * 24 * time.Duration(-initialSyncDays))
	lookbackWindowStart := lastDate.Add(time.Hour * 24 * time.Duration(-lookbackWindow))

	if t.Before(initialSyncStart) {
		currentStatus.Skipped++
		//debug("Row skipped. Too old", t)
		return
	}
	if initialState.Contains(t) {
		if t.Before(lookbackWindowStart) {
			currentStatus.Skipped++
			//debug("Row skipped. Already processed", t)
			return
		}
	}
	event := mp.NewEvent("$ad_spend", "", map[string]any{
		"$insert_id":      makeInsertId(payload),
		"time":            t,
		"$ad_platform":    payload.Source,
		"campaign_id":     payload.CampaignId,
		"$ad_cost":        payload.Cost,
		"$ad_clicks":      payload.Clicks,
		"$ad_impressions": payload.Impressions,
		"conversions":     payload.Conversions,
		"ad_group_id":     payload.GroupId,
		"ad_id":           payload.AdId,
		"campaign_name":   payload.CampaignName,
		"utm_campaign":    payload.UtmCampaign,
		"utm_source":      payload.UtmSource,
		"utm_medium":      payload.UtmMedium,
		"utm_term":        payload.UtmTerm,
		"utm_content":     payload.UtmContent,
	})
	batch = append(batch, event)
	processedRanges.Append(daterange.NewDateRange(t, t))
	if len(batch) >= batchSize {
		sendBatch(mp)
	}
}

func sendBatch(mp *mixpanel.ApiClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	if len(batch) > 0 {
		res, err := mp.Import(ctx, batch, mixpanel.ImportOptions{Compression: mixpanel.Gzip, Strict: false})
		if err != nil {
			currentStatus.Failed += len(batch)
			s, _ := json.Marshal(err)
			lerror(fmt.Sprintf("[%s] wrror importing %d rows.", lastProcessedDate, len(batch)), string(s))
		} else {
			if res.Code != 200 || res.NumRecordsImported == 0 {
				lerror(fmt.Sprintf("[%s] error importing %d rows. Code: %d Status: %+v", lastProcessedDate, len(batch), res.Code, res.Status))
				currentStatus.Failed += len(batch)
			} else {
				if !processedRanges.Equal(commitedState) {
					err = rpcClient.Set(stateKey, dateRangesToAny(processedRanges))
					if err != nil {
						lerror("Error saving state", err.Error())
					}
					commitedState = daterange.NewDateRanges(processedRanges.ToSlice()...)
				}
				currentStatus.Success += len(batch)
				info(fmt.Sprintf("[%s] %d rows sent", lastProcessedDate, len(batch)), res.Code, res.NumRecordsImported, res.Status)
			}
		}
		batch = nil
	}
}

func getStatus(date string) *Status {
	if _, ok := statuses[date]; !ok {
		statuses[date] = &Status{}
	}
	return statuses[date]
}

func makeInsertId(payload *RowPayload) string {
	builder := strings.Builder{}
	builder.WriteString(strings.ToUpper(payload.Source[0:1]))
	builder.WriteString("-")
	builder.WriteString(payload.Date)
	builder.WriteString("-")
	builder.WriteString(fmt.Sprint(payload.CampaignId))
	if payload.GroupId != nil {
		builder.WriteString("-")
		builder.WriteString(fmt.Sprint(payload.GroupId))
	}
	if payload.AdId != nil {
		builder.WriteString("-")
		builder.WriteString(fmt.Sprint(payload.AdId))
	}
	if builder.Len() > 36 {
		hasher := crypto.MD5.New()
		_, _ = hasher.Write([]byte(builder.String()))
		return strings.ToUpper(payload.Source[0:1]) + "-" + payload.Date + "-" + fmt.Sprintf("%x", hasher.Sum(nil))[0:23]
	} else {
		return builder.String()
	}

}

func logErr(err error) {
	log("error", err.Error())
}

func info(message string, params ...any) {
	log("info", message, params...)
}

func debug(message string, params ...any) {
	log("debug", message, params...)
}

func warn(message string, params ...any) {
	log("warn", message, params...)
}

func lerror(message string, params ...any) {
	log("error", message, params...)
}

func log(level string, message string, params ...any) {
	l := map[string]any{
		"level":   level,
		"message": message,
	}
	if len(params) > 0 {
		l["params"] = params
	}
	reply("log", l)
}

func reply(msgType string, payload any) {
	msg := Message{
		Type:      msgType,
		Direction: "reply",
		Payload:   payload,
	}
	data, _ := json.Marshal(&msg)
	fmt.Println(string(data))
}

func UnmarshalSchema(line string) map[string]any {
	var m map[string]any
	err := json.Unmarshal([]byte(line), &m)
	if err != nil {
		panic(err)
	}
	return m
}
