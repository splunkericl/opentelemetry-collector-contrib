package splunkhecexporter

import (
	"encoding/json"
	"errors"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// hecDataConverter is the base data converter between hecRequest and observability data(Logs, Traces and Metrics)
type hecDataConverter struct {
	config     *Config
	exportFunc hecRequestExportFunc
}

// Marshal marshals given exporterhelper.Request into bytes. Errors will be returned if provided type is not hecRequest
// This method is needed for writing data to persistent queue
func (hdc *hecDataConverter) Marshal(req exporterhelper.Request) ([]byte, error) {
	castedReq, ok := req.(*hecRequest)
	if !ok {
		return nil, errors.New("exporter request is not hecRequest")
	}
	return json.Marshal(castedReq.batchMap)
}

// Unmarshal un-marshals given bytes back to hecRequest
// This method is needed for reading data from persistent queue
func (hdc *hecDataConverter) Unmarshal(data []byte) (exporterhelper.Request, error) {
	var unmarshalledData hecBatchMap
	err := json.Unmarshal(data, &unmarshalledData)
	return newHecRequest(hdc.exportFunc, unmarshalledData), err
}
