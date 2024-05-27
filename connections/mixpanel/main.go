package main

import (
	"bufio"
	"context"
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
	Type      string         `json:"type"`
	Direction string         `json:"direction"`
	Payload   map[string]any `json:"payload"`
}

type RowPayload struct {
	Time        string  `mapstructure:"time"`
	Source      string  `mapstructure:"source"`
	CampaignId  any     `mapstructure:"campaign_id"`
	Cost        float64 `mapstructure:"cost"`
	Clicks      float64 `mapstructure:"clicks"`
	Impressions float64 `mapstructure:"impressions"`
	UtmSource   string  `mapstructure:"utm_source"`
	UtmCampaign string  `mapstructure:"utm_campaign"`
	UtmMedium   string  `mapstructure:"utm_medium"`
	UtmTerm     string  `mapstructure:"utm_term"`
	UtmContent  string  `mapstructure:"utm_content"`
}

var lookbackWindow = 2
var initialSyncDays = 30
var syncId string
var stateKey []string

var received int
var success int
var skipped int
var failed int

var rpcClient = NewRpcClient(os.Getenv("RPC_URL"))

var processedRanges *daterange.DateRanges
var startTime = time.Now()
var lastDate = startTime

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
			stream, ok := message.Payload["stream"]
			if !ok || stream != "AdData" {
				lerror("Unknown stream", stream)
				reply("halt", map[string]any{
					"message": fmt.Sprintf("Unknown stream: %s", stream),
				})
				os.Exit(1)
			}
			syncId, _ = message.Payload["syncId"].(string)
			creds, ok := message.Payload["connectionCredentials"].(map[string]any)
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
			stateKey = []string{"syncId=" + syncId, "type=mixpanel.state"}
			raw, err := rpcClient.Get(stateKey)
			if err != nil {
				lerror("Error getting state", err.Error())
			} else {
				processedRanges, err = dateRangesFromAny(raw)
				if err != nil {
					lerror("Error parsing state", err.Error())
				} else if processedRanges != nil && !processedRanges.IsZero() {
					info("State loaded", fmt.Sprint(processedRanges))
					lastDate = processedRanges.LastDate()
				}
			}
			if processedRanges == nil {
				p := daterange.NewDateRanges()
				processedRanges = &p
			}
			if residency == "EU" {
				mp = mixpanel.NewApiClient(projectToken, mixpanel.EuResidency())
			} else {
				mp = mixpanel.NewApiClient(projectToken)
			}
			info(fmt.Sprintf("Stream '%s' started. Residency: %s SyncId: %s InitialSyncDays: %d LookbackWindow: %d", stream, residency, syncId, initialSyncDays, lookbackWindow))
		case "end-stream":
			info("Received end-stream message. Bye!")
			reply("stream-result", map[string]any{
				"received": received,
				"skipped":  skipped,
				"failed":   failed,
				"success":  success,
			})
			time.AfterFunc(1000, func() {
				os.Exit(0)
			})
		case "row":
			received++
			var rowPayload RowPayload
			err = mapstructure.Decode(message.Payload["row"], &rowPayload)
			if err != nil {
				failed++
				lerror("Cannot parse row payload: "+line, err.Error())
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	t, err := time.Parse(time.RFC3339Nano, payload.Time)
	if err != nil {
		failed++
		lerror("Error parsing time: "+payload.Time, err.Error())
		return
	}
	tDate := toDateUTC(t)
	initialSyncStart := startTime.Truncate(time.Hour * 24).Add(time.Hour * 24 * time.Duration(-initialSyncDays))
	lookbackWindowStart := lastDate.Add(time.Hour * 24 * time.Duration(-lookbackWindow))

	if t.Before(initialSyncStart) {
		skipped++
		info("Row skipped. Too old", t)
		return
	}
	if processedRanges.Contains(tDate) {
		if t.Before(lookbackWindowStart) {
			skipped++
			info("Row skipped. Already processed", tDate)
			return
		}
	}
	status, err := mp.Import(ctx, []*mixpanel.Event{mp.NewEvent("Ad Data", "", map[string]any{
		"$insert_id":   fmt.Sprintf("G-%s-%v", t.Format(time.DateOnly), payload.CampaignId),
		"time":         t,
		"source":       payload.Source,
		"campaign_id":  payload.CampaignId,
		"cost":         payload.Cost,
		"clicks":       payload.Clicks,
		"impressions":  payload.Impressions,
		"utm_campaign": payload.UtmCampaign,
		"utm_source":   payload.UtmSource,
		"utm_medium":   payload.UtmMedium,
		"utm_term":     payload.UtmTerm,
		"utm_content":  payload.UtmContent,
	})}, mixpanel.ImportOptions{Compression: mixpanel.Gzip, Strict: false})
	if err != nil {
		failed++
		s, _ := json.Marshal(err)
		lerror("Error importing row:", string(s))
	} else {
		if status.Code != 200 || status.NumRecordsImported == 0 {
			lerror(fmt.Sprintf("Error importing row. Code: %d Status: %+v", status.Code, status.Status))
			failed++
		} else {
			processedRanges.Append(daterange.NewDateRange(t, t))
			err = rpcClient.Set(stateKey, dateRangesToAny(processedRanges))
			if err != nil {
				lerror("Error saving state", err.Error())
			}
			success++
			info("Row imported", status.Code, status.NumRecordsImported, status.Status)
		}
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

func reply(msgType string, payload map[string]any) {
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

// toDateUTC truncate a time to the date and set UTC location.
func toDateUTC(t time.Time) time.Time {
	// the correct way to truncate a time to the date is to use
	// time.Date with the zero values for the time components.
	// The commonly used time.Truncate(24 * time.Hour) method does not work
	// correctly for all cases, for example it ignores DST.
	return time.Date(
		t.Year(), t.Month(), t.Day(),
		0, 0, 0, 0,
		time.UTC)
}
