package sink

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/stretchr/testify/require"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/event"
)

const (
	catalystSizeTableSeries = 9_155
	catalystWaitTableRows   = 4_750
	catalystWaitMetrics     = 4
	catalystFixedSeries     = 100
	catalystTotalSeries     = catalystSizeTableSeries + catalystWaitTableRows*catalystWaitMetrics + catalystFixedSeries
)

type catalystIntakeStats struct {
	requests       int
	rejected       int
	accepted       int
	totalWire      int64
	totalRaw       int64
	maxWire        int
	maxRaw         int
	acceptedSeries []datadogV2.MetricSeries
}

// catalystIntake simulates the two independent Datadog v2 series limits and
// records only successfully accepted payloads. It is shared by the correctness
// test and benchmarks so both implementations see identical intake behavior.
type catalystIntake struct {
	mu              sync.Mutex
	maxCompressed   int
	maxDecompressed int
	stats           catalystIntakeStats
}

func newCatalystIntake() *catalystIntake {
	return &catalystIntake{
		maxCompressed:   datadogMaxCompressedPayloadSize,
		maxDecompressed: datadogMaxDecompressedPayloadSize,
	}
}

func (i *catalystIntake) RoundTrip(req *http.Request) (*http.Response, error) {
	wire, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	raw := wire
	compressed := req.Header.Get("Content-Encoding") == "gzip"
	if compressed {
		reader, err := gzip.NewReader(bytes.NewReader(wire))
		if err != nil {
			return nil, err
		}
		raw, err = io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}

	rejected := len(wire) > i.maxCompressed || (compressed && len(raw) >= i.maxDecompressed)

	i.mu.Lock()
	i.stats.requests++
	i.stats.totalWire += int64(len(wire))
	i.stats.totalRaw += int64(len(raw))
	if len(wire) > i.stats.maxWire {
		i.stats.maxWire = len(wire)
	}
	if len(raw) > i.stats.maxRaw {
		i.stats.maxRaw = len(raw)
	}
	if rejected {
		i.stats.rejected++
		i.mu.Unlock()
		return catalystHTTPResponse(http.StatusRequestEntityTooLarge, `{"errors":["Payload too large"]}`), nil
	}

	var payload datadogV2.MetricPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		i.mu.Unlock()
		return nil, err
	}
	i.stats.accepted++
	i.stats.acceptedSeries = append(i.stats.acceptedSeries, payload.Series...)
	i.mu.Unlock()

	return catalystHTTPResponse(http.StatusAccepted, `{"errors":[]}`), nil
}

func catalystHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(bytes.NewBufferString(body)),
	}
}

func (i *catalystIntake) snapshot() catalystIntakeStats {
	i.mu.Lock()
	defer i.mu.Unlock()
	stats := i.stats
	stats.acceptedSeries = append([]datadogV2.MetricSeries(nil), i.stats.acceptedSeries...)
	return stats
}

func catalystMetricSeries() []datadogV2.MetricSeries {
	return catalystMetricSeriesSized(catalystSizeTableSeries, catalystWaitTableRows, catalystFixedSeries)
}

func catalystCommonTags() []string {
	return []string{
		"infrastructure_type:aurora",
		"org:database",
		"team:aurora-mysql",
		"cost_center:1234",
		"service:doppler",
		"env:production",
		"blip_metric_version:2",
		"metacluster:catalyst-production-aurora",
		"cluster:catalyst-production-aurora-usw2-001",
		"app:blippy-catalyst-production-aurora-001",
		"dc:us-west-2",
		"region:us-west-2",
		"odsgroup:catalyst-production-aurora-001",
		"hostname:catalyst-production-aurora-001.cluster-abcdefghijkl.us-west-2.rds.amazonaws.com",
		"host:catalyst-production-aurora-001-instance-1",
		"physical_host:unknown",
		"dbtype:mysql",
		"storage_type:aurora",
		"channel:stable",
	}
}

func catalystResources() []datadogV2.MetricResource {
	return []datadogV2.MetricResource{{
		Name: datadog.PtrString("catalyst-production-aurora-001-instance-1"),
		Type: datadog.PtrString("host"),
	}}
}

func catalystMetricSeriesSized(sizeTableSeries, waitTableRows, fixedSeries int) []datadogV2.MetricSeries {
	commonTags := catalystCommonTags()
	resources := catalystResources()

	series := make([]datadogV2.MetricSeries, 0, sizeTableSeries+waitTableRows*catalystWaitMetrics+fixedSeries)
	appendTableSeries := func(metric string, metricType datadogV2.MetricIntakeType, table int) {
		tags := make([]string, len(commonTags), len(commonTags)+2)
		copy(tags, commonTags)
		tags = append(tags,
			fmt.Sprintf("db:database_%03d", table%160),
			fmt.Sprintf("tbl:application_table_%05d", table),
		)
		series = append(series, datadogV2.MetricSeries{
			Metric: metric,
			Type:   metricType.Ptr(),
			Points: []datadogV2.MetricPoint{{
				Timestamp: datadog.PtrInt64(1_784_396_000),
				Value:     datadog.PtrFloat64(float64(table % 1000)),
			}},
			Resources: resources,
			Tags:      tags,
		})
	}

	for table := 0; table < sizeTableSeries; table++ {
		appendTableSeries("mysql.size.table.bytes", datadogV2.METRICINTAKETYPE_GAUGE, table)
	}

	waitMetrics := []string{
		"mysql.wait.io.table.count_fetch",
		"mysql.wait.io.table.time_fetch",
		"mysql.wait.io.table.count_insert",
		"mysql.wait.io.table.time_insert",
	}
	for table := 0; table < waitTableRows; table++ {
		for _, metric := range waitMetrics {
			appendTableSeries(metric, datadogV2.METRICINTAKETYPE_COUNT, table)
		}
	}

	for metric := 0; metric < fixedSeries; metric++ {
		series = append(series, datadogV2.MetricSeries{
			Metric: fmt.Sprintf("mysql.status.synthetic_%03d", metric),
			Type:   datadogV2.METRICINTAKETYPE_GAUGE.Ptr(),
			Points: []datadogV2.MetricPoint{{
				Timestamp: datadog.PtrInt64(1_784_396_000),
				Value:     datadog.PtrFloat64(float64(metric)),
			}},
			Resources: resources,
			Tags:      commonTags,
		})
	}

	return series
}

func catalystMetricsMetadata() *blip.Metrics {
	return &blip.Metrics{
		MonitorId: "catalyst-production-aurora-001-instance-1",
		Plan:      "dd-mysql",
		Level:     "data-size",
		Interval:  1,
	}
}

func catalystBlipMetrics(series []datadogV2.MetricSeries) *blip.Metrics {
	metrics := catalystMetricsMetadata()
	metrics.Begin = time.Unix(1_784_396_000, 0)
	metrics.End = metrics.Begin.Add(time.Second)
	metrics.Values = map[string][]blip.MetricValue{}
	commonTagCount := len(catalystCommonTags())

	for _, item := range series {
		separator := strings.LastIndexByte(item.Metric, '.')
		if separator < 1 || separator == len(item.Metric)-1 {
			panic("invalid Catalyst metric name: " + item.Metric)
		}
		metricType := blip.GAUGE
		if item.Type != nil && *item.Type == datadogV2.METRICINTAKETYPE_COUNT {
			metricType = blip.DELTA_COUNTER
		}
		value := blip.MetricValue{
			Name:  item.Metric[separator+1:],
			Value: *item.Points[0].Value,
			Type:  metricType,
		}
		if len(item.Tags) > commonTagCount {
			value.Group = map[string]string{}
			for _, tag := range item.Tags[commonTagCount:] {
				key, val, ok := strings.Cut(tag, ":")
				if !ok {
					panic("invalid Catalyst tag: " + tag)
				}
				value.Group[key] = val
			}
		}
		domain := item.Metric[:separator]
		metrics.Values[domain] = append(metrics.Values[domain], value)
	}
	return metrics
}

func canonicalCatalystSeries(series []datadogV2.MetricSeries) []string {
	encoded := make([]string, len(series))
	for i := range series {
		item := series[i]
		item.Tags = append([]string(nil), item.Tags...)
		sort.Strings(item.Tags)
		data, err := json.Marshal(item)
		if err != nil {
			panic(err)
		}
		encoded[i] = string(data)
	}
	sort.Strings(encoded)
	return encoded
}

// materializeDatadogSeries models the pre-streaming conversion path: convert
// the complete Blip batch into one MetricSeries slice before payload sizing.
func materializeDatadogSeries(sender *Datadog, metrics *blip.Metrics) []datadogV2.MetricSeries {
	domains := make([]string, 0, len(metrics.Values))
	total := 0
	for domain, values := range metrics.Values {
		domains = append(domains, domain)
		total += len(values)
	}
	sort.Strings(domains)
	series := make([]datadogV2.MetricSeries, 0, total)
	for _, domain := range domains {
		for _, value := range metrics.Values[domain] {
			converted, ok := sender.datadogMetricSeries(metrics, domain, value)
			if ok {
				series = append(series, converted)
			}
		}
	}
	return series
}

func catalystAPIClient(intake *catalystIntake) *datadog.APIClient {
	cfg := datadog.NewConfiguration()
	cfg.HTTPClient = &http.Client{Transport: intake}
	cfg.Compress = true
	return datadog.NewAPIClient(cfg)
}

// legacyDatadogSender is the previous reactive autosizer, retained only in the
// test harness so old and new behavior can be measured against the same data.
type legacyDatadogSender struct {
	api                  *datadogV2.MetricsApi
	maxMetricsPerRequest int
	maxPayloadSize       int
}

func newLegacyDatadogSender(intake *catalystIntake) *legacyDatadogSender {
	return &legacyDatadogSender{
		api:                  datadogV2.NewMetricsApi(catalystAPIClient(intake)),
		maxMetricsPerRequest: math.MaxInt32,
		maxPayloadSize:       datadogMaxCompressedPayloadSize,
	}
}

func (s *legacyDatadogSender) Send(ctx context.Context, series []datadogV2.MetricSeries) error {
	ctx = context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{
		"apiKeyAuth": {Key: "test-api-key"},
	})
	localMax := s.maxMetricsPerRequest

	for start := 0; start < len(series); {
		end := start + localMax
		if end > len(series) {
			end = len(series)
		}

		options := *datadogV2.NewSubmitMetricsOptionalParameters()
		options.ContentEncoding = datadogV2.METRICCONTENTENCODING_GZIP.Ptr()
		_, response, err := s.api.SubmitMetrics(ctx, *datadogV2.NewMetricPayload(series[start:end]), options)
		if err != nil {
			if response == nil || response.StatusCode != http.StatusRequestEntityTooLarge {
				return err
			}
			if localMax == 1 {
				return err
			}
			localMax, err = s.estimateMaxMetricsPerRequest(series[start:end], localMax)
			if err != nil {
				return err
			}
			continue
		}
		start = end
	}

	if localMax < s.maxMetricsPerRequest {
		s.maxMetricsPerRequest = localMax
	}
	return nil
}

func (s *legacyDatadogSender) estimateMaxMetricsPerRequest(series []datadogV2.MetricSeries, currentMax int) (int, error) {
	data, err := json.Marshal(series)
	if err != nil {
		return 0, err
	}

	var compressed bytes.Buffer
	w := zlib.NewWriter(&compressed)
	if _, err := w.Write(data); err != nil {
		return 0, err
	}
	if err := w.Close(); err != nil {
		return 0, err
	}

	estimatedMetricSize := compressed.Len() / len(series)
	if estimatedMetricSize == 0 {
		return 0, fmt.Errorf("legacy estimator produced a zero-byte metric estimate")
	}
	estimatedMax := (s.maxPayloadSize - 300) / estimatedMetricSize
	if estimatedMax >= currentMax {
		estimatedMax = int(float32(currentMax) * 0.9)
	}
	if estimatedMax < 1 {
		estimatedMax = 1
	}
	return estimatedMax, nil
}

func newCatalystDatadogSender(intake *catalystIntake) *Datadog {
	sink := &Datadog{
		monitorId:     "catalyst-production-aurora-001-instance-1",
		event:         event.MonitorReceiver{MonitorId: "catalyst-production-aurora-001-instance-1"},
		compress:      true,
		tags:          catalystCommonTags(),
		resources:     catalystResources(),
		payloadLimits: defaultDatadogPayloadLimits(),
		submitter: &datadogAPISubmitter{
			client: catalystAPIClient(intake),
			apiKey: "test-api-key",
		},
	}
	sink.maxSeriesPerRequest.Store(int64(sink.payloadLimits.maxSeries))
	return sink
}

func TestDatadogCatalystHarnessEquivalent(t *testing.T) {
	if os.Getenv("BLIP_CATALYST_HARNESS") != "1" {
		t.Skip("set BLIP_CATALYST_HARNESS=1 to run the full 28,255-series comparison")
	}
	series := catalystMetricSeries()
	require.Len(t, series, catalystTotalSeries)

	legacyIntake := newCatalystIntake()
	legacy := newLegacyDatadogSender(legacyIntake)
	require.NoError(t, legacy.Send(context.Background(), series))
	legacyStats := legacyIntake.snapshot()

	newIntake := newCatalystIntake()
	modern := newCatalystDatadogSender(newIntake)
	require.NoError(t, modern.sendAPI(context.Background(), series, catalystMetricsMetadata()))
	newStats := newIntake.snapshot()

	finalIntake := newCatalystIntake()
	final := newCatalystDatadogSender(finalIntake)
	realMetrics := catalystBlipMetrics(series)
	require.NoError(t, final.Send(context.Background(), realMetrics))
	finalStats := finalIntake.snapshot()

	legacyCanonical := canonicalCatalystSeries(legacyStats.acceptedSeries)
	require.Equal(t, legacyCanonical, canonicalCatalystSeries(newStats.acceptedSeries), "pre and mid senders must submit identical series and points")
	require.Equal(t, legacyCanonical, canonicalCatalystSeries(finalStats.acceptedSeries), "pre and final senders must submit identical series and points")

	require.Equal(t, catalystTotalSeries, len(legacyStats.acceptedSeries))
	require.Equal(t, catalystTotalSeries, len(newStats.acceptedSeries))
	require.Equal(t, catalystTotalSeries, len(finalStats.acceptedSeries))
	require.Greater(t, legacyStats.rejected, 0, "legacy harness should exercise reactive 413 autosizing")
	require.Zero(t, newStats.rejected, "new sender should size payloads before submission")
	require.Zero(t, finalStats.rejected, "streaming sender should size payloads before submission")
	require.Less(t, newStats.requests, legacyStats.requests)
	require.Equal(t, newStats.requests, finalStats.requests, "bounded conversion should not add payloads")
	require.Less(t, newStats.maxRaw, datadogMaxDecompressedPayloadSize)
	require.LessOrEqual(t, newStats.maxWire, datadogMaxCompressedPayloadSize)
	require.Less(t, finalStats.maxRaw, datadogMaxDecompressedPayloadSize)
	require.LessOrEqual(t, finalStats.maxWire, datadogMaxCompressedPayloadSize)

	t.Logf("legacy: requests=%d rejected=%d accepted=%d total-wire=%d total-raw=%d max-wire=%d max-raw=%d",
		legacyStats.requests, legacyStats.rejected, legacyStats.accepted,
		legacyStats.totalWire, legacyStats.totalRaw, legacyStats.maxWire, legacyStats.maxRaw)
	t.Logf("mid: requests=%d rejected=%d accepted=%d total-wire=%d total-raw=%d max-wire=%d max-raw=%d",
		newStats.requests, newStats.rejected, newStats.accepted,
		newStats.totalWire, newStats.totalRaw, newStats.maxWire, newStats.maxRaw)
	t.Logf("final: requests=%d rejected=%d accepted=%d total-wire=%d total-raw=%d max-wire=%d max-raw=%d",
		finalStats.requests, finalStats.rejected, finalStats.accepted,
		finalStats.totalWire, finalStats.totalRaw, finalStats.maxWire, finalStats.maxRaw)
}

func TestDatadogPayloadAlgorithmsEquivalent(t *testing.T) {
	series := catalystMetricSeriesSized(1_000, 0, 0)

	legacyIntake := newCatalystIntake()
	legacyIntake.maxCompressed = 50_000
	legacyIntake.maxDecompressed = 150_000
	legacy := newLegacyDatadogSender(legacyIntake)
	legacy.maxPayloadSize = legacyIntake.maxCompressed
	require.NoError(t, legacy.Send(context.Background(), series))
	legacyStats := legacyIntake.snapshot()

	newIntake := newCatalystIntake()
	newIntake.maxCompressed = legacyIntake.maxCompressed
	newIntake.maxDecompressed = legacyIntake.maxDecompressed
	modern := newCatalystDatadogSender(newIntake)
	modern.payloadLimits = datadogPayloadLimits{
		maxCompressed:      newIntake.maxCompressed,
		maxDecompressed:    newIntake.maxDecompressed,
		targetCompressed:   newIntake.maxCompressed * 9 / 10,
		targetDecompressed: newIntake.maxDecompressed * 9 / 10,
		maxSeries:          len(series),
	}
	modern.maxSeriesPerRequest.Store(int64(len(series)))
	require.NoError(t, modern.sendAPI(context.Background(), series, catalystMetricsMetadata()))
	newStats := newIntake.snapshot()

	finalIntake := newCatalystIntake()
	finalIntake.maxCompressed = legacyIntake.maxCompressed
	finalIntake.maxDecompressed = legacyIntake.maxDecompressed
	final := newCatalystDatadogSender(finalIntake)
	final.payloadLimits = modern.payloadLimits
	final.maxSeriesPerRequest.Store(int64(len(series)))
	require.NoError(t, final.Send(context.Background(), catalystBlipMetrics(series)))
	finalStats := finalIntake.snapshot()

	legacyCanonical := canonicalCatalystSeries(legacyStats.acceptedSeries)
	require.Equal(t, legacyCanonical, canonicalCatalystSeries(newStats.acceptedSeries))
	require.Equal(t, legacyCanonical, canonicalCatalystSeries(finalStats.acceptedSeries))
	require.Greater(t, legacyStats.rejected, 0)
	require.Zero(t, newStats.rejected)
	require.Zero(t, finalStats.rejected)
	require.Equal(t, newStats.requests, finalStats.requests)
}

func BenchmarkDatadogCatalystPayload(b *testing.B) {
	series := catalystMetricSeries()
	metadata := catalystMetricsMetadata()
	realMetrics := catalystBlipMetrics(series)

	b.Run("legacy-reactive-autosizer", func(b *testing.B) {
		b.ReportAllocs()
		var totals catalystIntakeStats
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			intake := newCatalystIntake()
			sender := newLegacyDatadogSender(intake)
			converter := newCatalystDatadogSender(intake)
			b.StartTimer()

			materialized := materializeDatadogSeries(converter, realMetrics)
			if err := sender.Send(context.Background(), materialized); err != nil {
				b.Fatal(err)
			}

			b.StopTimer()
			stats := intake.snapshot()
			totals.requests += stats.requests
			totals.rejected += stats.rejected
			totals.accepted += stats.accepted
			totals.totalWire += stats.totalWire
			totals.totalRaw += stats.totalRaw
			b.StartTimer()
		}
		b.ReportMetric(float64(totals.requests)/float64(b.N), "requests/op")
		b.ReportMetric(float64(totals.rejected)/float64(b.N), "rejected/op")
		b.ReportMetric(float64(totals.accepted)/float64(b.N), "accepted/op")
		b.ReportMetric(float64(len(series)), "series/op")
		b.ReportMetric(float64(len(series)), "max-converted-series/op")
		b.ReportMetric(float64(totals.totalWire)/float64(b.N), "wire-bytes/op")
		b.ReportMetric(float64(totals.totalRaw)/float64(b.N), "raw-bytes/op")
	})

	b.Run("byte-budgeted-prepared-payload", func(b *testing.B) {
		b.ReportAllocs()
		var totals catalystIntakeStats
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			intake := newCatalystIntake()
			sender := newCatalystDatadogSender(intake)
			b.StartTimer()

			materialized := materializeDatadogSeries(sender, realMetrics)
			if err := sender.sendAPI(context.Background(), materialized, metadata); err != nil {
				b.Fatal(err)
			}

			b.StopTimer()
			stats := intake.snapshot()
			totals.requests += stats.requests
			totals.rejected += stats.rejected
			totals.accepted += stats.accepted
			totals.totalWire += stats.totalWire
			totals.totalRaw += stats.totalRaw
			b.StartTimer()
		}
		b.ReportMetric(float64(totals.requests)/float64(b.N), "requests/op")
		b.ReportMetric(float64(totals.rejected)/float64(b.N), "rejected/op")
		b.ReportMetric(float64(totals.accepted)/float64(b.N), "accepted/op")
		b.ReportMetric(float64(len(series)), "series/op")
		b.ReportMetric(float64(len(series)), "max-converted-series/op")
		b.ReportMetric(float64(totals.totalWire)/float64(b.N), "wire-bytes/op")
		b.ReportMetric(float64(totals.totalRaw)/float64(b.N), "raw-bytes/op")
	})

	b.Run("checkpointed-streaming-conversion", func(b *testing.B) {
		b.ReportAllocs()
		var totals catalystIntakeStats
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			intake := newCatalystIntake()
			sender := newCatalystDatadogSender(intake)
			b.StartTimer()

			if err := sender.Send(context.Background(), realMetrics); err != nil {
				b.Fatal(err)
			}

			b.StopTimer()
			stats := intake.snapshot()
			totals.requests += stats.requests
			totals.rejected += stats.rejected
			totals.accepted += stats.accepted
			totals.totalWire += stats.totalWire
			totals.totalRaw += stats.totalRaw
			b.StartTimer()
		}
		b.ReportMetric(float64(totals.requests)/float64(b.N), "requests/op")
		b.ReportMetric(float64(totals.rejected)/float64(b.N), "rejected/op")
		b.ReportMetric(float64(totals.accepted)/float64(b.N), "accepted/op")
		b.ReportMetric(float64(len(series)), "series/op")
		b.ReportMetric(float64(defaultDatadogPayloadLimits().maxSeries), "max-converted-series/op")
		b.ReportMetric(float64(totals.totalWire)/float64(b.N), "wire-bytes/op")
		b.ReportMetric(float64(totals.totalRaw)/float64(b.N), "raw-bytes/op")
	})
}
