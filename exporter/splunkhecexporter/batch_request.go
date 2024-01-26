package splunkhecexporter

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"golang.org/x/exp/maps"
)

const (
	encodingHecTokenKey        = "hecToken"
	encodingIndexKey           = "index"
	encodingIsProfilingDataKey = "isProfilingData"
)

type hecBatchMap map[hecScope][]*splunk.Event
type hecRequestExportFunc func(context.Context, hecBatchMap) error

type hecScope struct {
	hecToken        string
	index           string
	isProfilingData bool
}

// MarshalText marshal hecScope into bytes so channelInfo can be used for a map key
func (c hecScope) MarshalText() (text []byte, err error) {
	encodingMap := map[string]interface{}{
		encodingHecTokenKey:        c.hecToken,
		encodingIndexKey:           c.index,
		encodingIsProfilingDataKey: c.isProfilingData,
	}
	return json.Marshal(encodingMap)
}

// UnmarshalText un-marshal given bytes to hecScope
func (c *hecScope) UnmarshalText(text []byte) error {
	var encodingMap map[string]interface{}
	if err := json.Unmarshal(text, &encodingMap); err != nil {
		return err
	}

	c.hecToken = encodingMap[encodingHecTokenKey].(string)
	c.index = encodingMap[encodingIndexKey].(string)
	c.isProfilingData = encodingMap[encodingIndexKey].(bool)
	return nil
}

type hecRequest struct {
	exportFunc hecRequestExportFunc
	batchMap   hecBatchMap
	itemCount  int
}

func (h *hecRequest) ItemsCount() int {
	return h.itemCount
}

func newHecRequest(exportFunc hecRequestExportFunc, batchData hecBatchMap) *hecRequest {
	itemCount := 0
	for _, data := range batchData {
		itemCount += len(data)
	}
	return &hecRequest{
		exportFunc: exportFunc,
		batchMap:   batchData,
		itemCount:  itemCount,
	}
}

func (h *hecRequest) Export(ctx context.Context) error {
	return h.exportFunc(ctx, h.batchMap)
}

func (h *hecRequest) merge(other *hecRequest) exporterhelper.Request {
	mergedMap := h.getMergedHecBatchMap(other)
	return newHecRequest(h.exportFunc, mergedMap)
}

func (h *hecRequest) mergeSplit(other *hecRequest, maxItems int) []exporterhelper.Request {
	combinedMap := h.getMergedHecBatchMap(other)

	var result []exporterhelper.Request // the final returned output of split logs
	var carryover hecBatchMap           // between partitions, there may be unfilled chunks. carryover may last through many partitions.
	totalCarryoverSize := 0             // precompute total items carried over to avoid constant recalculation
	for scopeKey, events := range combinedMap {
		// idea here is take greedy approach. take as many items in the current array (entry) as possible,
		// up until maxItems. if the end is reached, carryover is stored.
		for entryIndex := 0; entryIndex < len(events); entryIndex += maxItems {
			// case 1: the desired number of items exceeds what remains in the array.
			// take some carryover and combine with the next array.
			if entryIndex+maxItems-totalCarryoverSize > len(events) {
				if carryover == nil {
					carryover = hecBatchMap{}
				}
				carryover[scopeKey] = events[entryIndex:]
				totalCarryoverSize += len(events[entryIndex:])
				// case 2: the desired number of items exists within the rest of the array.
				// no news carryover needed. if previous carryover exists, add it to the new entry.
			} else {
				newEntry := hecBatchMap{}
				if carryover != nil {
					newEntry = carryover // build off previous carryover
				}
				newEntry[scopeKey] = events[entryIndex : entryIndex+maxItems-totalCarryoverSize]
				if carryover != nil {
					// after each loop, entryIndex is incremented by maxItems, and carryover may reduce
					// how many items are taken from current array. subtract this to offset the next increment.
					entryIndex -= totalCarryoverSize
				}
				carryover = nil
				totalCarryoverSize = 0
				result = append(result, newHecRequest(h.exportFunc, newEntry))
			}
		}
	}
	if totalCarryoverSize > 0 {
		result = append(result, newHecRequest(h.exportFunc, carryover))
	}
	return result
}

func (h *hecRequest) getMergedHecBatchMap(other *hecRequest) hecBatchMap {
	combinedMap := hecBatchMap{}
	// copy instead of modify since exporter request does not allow modifying original request
	maps.Copy(combinedMap, h.batchMap)
	if other != nil {
		for key, events := range other.batchMap {
			if existingEvents, ok := combinedMap[key]; ok {
				combinedMap[key] = append(existingEvents, events...)
			} else {
				combinedMap[key] = events
			}
		}
	}
	return combinedMap
}

func hecMergeFunc(_ context.Context, req1 exporterhelper.Request, req2 exporterhelper.Request) (exporterhelper.Request, error) {
	castedReq1, ok1 := req1.(*hecRequest)
	castedReq2, ok2 := req2.(*hecRequest)
	if !ok1 || !ok2 {
		return nil, errors.New("non hecRequest provided to hecMergeFunc")
	}
	return castedReq1.merge(castedReq2), nil
}

func hecMergeSplitFunc(_ context.Context, optionalReq exporterhelper.Request, req exporterhelper.Request, maxItems int) ([]exporterhelper.Request, error) {
	castedReq1, ok1 := req.(*hecRequest)

	var castedReq2 *hecRequest
	ok2 := true
	if optionalReq != nil {
		castedReq2, ok2 = optionalReq.(*hecRequest)
	}
	if !ok1 || !ok2 {
		return nil, errors.New("non hecRequest provided to hecMergeSplitFunc")
	}
	return castedReq1.mergeSplit(castedReq2, maxItems), nil
}
