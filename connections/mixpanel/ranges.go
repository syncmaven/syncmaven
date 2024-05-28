package main

import (
	"encoding/json"
	"fmt"
	daterange "github.com/felixenescu/date-range"
	"time"
)

func marshalDateRanges(dr daterange.DateRanges) ([]byte, error) {
	b, err := json.Marshal(dateRangesToAny(dr))
	if err != nil {
		return nil, fmt.Errorf("error marshalling date ranges: %v", err)
	}
	return b, nil
}

func dateRangesToAny(dr daterange.DateRanges) []any {
	arr := make([]any, dr.Len())
	for i, r := range dr.ToSlice() {
		if r.From() == r.To() {
			arr[i] = r.From().Format(time.DateOnly)
		} else {
			arr[i] = []string{r.From().Format(time.DateOnly), r.To().Format(time.DateOnly)}
		}
	}
	return arr
}

func dateRangesFromAny(raw any) (daterange.DateRanges, error) {
	nl := daterange.NewDateRanges()
	switch arr := raw.(type) {
	case []any:
		dr := daterange.NewDateRanges()
		for _, r := range arr {
			switch mr := r.(type) {
			case string:
				start, err := time.Parse(time.DateOnly, mr)
				if err != nil {
					return nl, fmt.Errorf("error parsing date: %v", err)
				}
				dr.Append(daterange.NewDateRange(start, start))
			case []any:
				if len(mr) != 2 {
					return nl, fmt.Errorf("expected array of length 2, got %v", mr)
				}
				s, _ := mr[0].(string)
				e, _ := mr[1].(string)
				start, err := time.Parse(time.DateOnly, s)
				if err != nil {
					return nl, fmt.Errorf("error parsing start date: %v", err)
				}
				end, err := time.Parse(time.DateOnly, e)
				if err != nil {
					return nl, fmt.Errorf("error parsing start date: %v", err)
				}
				dr.Append(daterange.NewDateRange(start, end))
			default:
				return nl, fmt.Errorf("expected array, got %T", r)
			}
		}
		return dr, nil
	case map[string]any:
		if len(arr) > 0 {
			return nl, fmt.Errorf("expected array of ranges, got map: %+v", arr)
		} else {
			return nl, nil
		}
	case nil:
		return nl, nil
	default:
		return nl, fmt.Errorf("expected array of ranges, got %T", raw)
	}

}

func unmarshalDateRanges(b []byte) (daterange.DateRanges, error) {
	var raw any
	err := json.Unmarshal(b, &raw)
	if err != nil {
		return daterange.NewDateRanges(), fmt.Errorf("error unmarshalling date ranges: %v", err)
	}
	return dateRangesFromAny(raw)
}
