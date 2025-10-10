package main

import (
	"crypto"
	"fmt"
	"strings"
)

type Message struct {
	Type      string `json:"type"`
	Direction string `json:"direction"`
	Payload   any    `json:"payload"`
}

type Status struct {
	Received int `json:"received"`
	Success  int `json:"success"`
	Skipped  int `json:"skipped"`
	Failed   int `json:"failed"`
}

type RowPayload = map[string]any

//	Date         string  `mapstructure:"date"`
//	Source       string  `mapstructure:"source"`
//	CampaignId   any     `mapstructure:"campaign_id"`
//	CampaignName string  `mapstructure:"campaign_name"`
//	GroupId      any     `mapstructure:"group_id"`
//	AdId         any     `mapstructure:"ad_id"`
//	AdSetId      any     `mapstructure:"ad_set_id"`
//	Cost         float64 `mapstructure:"cost"`
//	Clicks       float64 `mapstructure:"clicks"`
//	Impressions  float64 `mapstructure:"impressions"`
//	Conversions  float64 `mapstructure:"conversions"`
//	UtmSource    string  `mapstructure:"utm_source"`
//	UtmCampaign  string  `mapstructure:"utm_campaign"`
//	UtmMedium    string  `mapstructure:"utm_medium"`
//	UtmTerm      string  `mapstructure:"utm_term"`
//	UtmContent   string  `mapstructure:"utm_content"`

var nameMappings = map[string]string{
	"source":      "$ad_platform",
	"cost":        "$ad_cost",
	"clicks":      "$ad_clicks",
	"impressions": "$ad_impressions",
	"group_id":    "ad_group_id",
}

func Adapted(r RowPayload) map[string]any {
	adapted := map[string]any{}
	for k, v := range r {
		if newKey, ok := nameMappings[k]; ok {
			adapted[newKey] = v
		} else {
			adapted[k] = v
		}
	}
	if _, ok := adapted["$insert_id"]; !ok {
		adapted["$insert_id"] = makeInsertId(r)
	}
	return adapted
}

func Date(r RowPayload) string {
	if v, ok := r["date"]; ok {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func makeInsertId(r RowPayload) string {
	source, _ := r["source"].(string)
	if len(source) > 1 {
		source = source[0:1]
	}
	date := Date(r)
	builder := strings.Builder{}
	builder.WriteString(strings.ToUpper(source))
	builder.WriteString("-")
	builder.WriteString(date)
	builder.WriteString("-")
	builder.WriteString(fmt.Sprint(r["campaign_id"]))
	if groupId, ok := r["group_id"]; ok && groupId != nil {
		builder.WriteString("-")
		builder.WriteString(fmt.Sprint(groupId))
	}
	if adId, ok := r["ad_id"]; ok && adId != nil {
		builder.WriteString("-")
		builder.WriteString(fmt.Sprint(adId))
	}
	hasher := crypto.MD5.New()
	_, _ = hasher.Write([]byte(builder.String()))
	return strings.ToUpper(source) + "-" + date + "-" + fmt.Sprintf("%x", hasher.Sum(nil))[0:23]
}
