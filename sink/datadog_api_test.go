package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/stretchr/testify/require"
)

type rejectThenFailSubmitter struct {
	calls int
}

func (s *rejectThenFailSubmitter) Submit(context.Context, preparedDatadogPayload) (datadogSubmitResult, error) {
	s.calls++
	if s.calls == 1 {
		return datadogSubmitResult{statusCode: http.StatusRequestEntityTooLarge}, errors.New("payload too large")
	}
	return datadogSubmitResult{}, errors.New("network failure")
}

func TestPrepareDatadogPayloadHonorsCompressedLimit(t *testing.T) {
	series := catalystMetricSeriesSized(1_000, 0, 0)
	limits := datadogPayloadLimits{
		maxCompressed:      1_000,
		maxDecompressed:    1_000_000,
		targetCompressed:   900,
		targetDecompressed: 900_000,
		maxSeries:          len(series),
	}

	payload, end, err := prepareDatadogPayload(context.Background(), series, 0, len(series), true, limits)
	require.NoError(t, err)
	require.Less(t, end, len(series), "compressed byte budget should split the input")
	require.LessOrEqual(t, payload.compressedBytes, limits.targetCompressed)
	require.LessOrEqual(t, payload.uncompressedBytes, limits.targetDecompressed)

	reader, err := gzip.NewReader(bytes.NewReader(payload.body))
	require.NoError(t, err)
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	var decoded datadogV2.MetricPayload
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded.Series, payload.seriesCount)
	require.Equal(t, series[:end], decoded.Series)
}

func TestDatadog413FallbackHalvesAndPersistsLimit(t *testing.T) {
	series := catalystMetricSeriesSized(1_000, 0, 0)
	intake := newCatalystIntake()
	// Simulate an effective server limit lower than the documented limit. The
	// locally valid first payload should receive 413 and trigger bounded halving.
	intake.maxDecompressed = 150_000

	sender := newCatalystDatadogSender(intake)
	require.NoError(t, sender.sendAPI(context.Background(), series, catalystMetricsMetadata()))

	stats := intake.snapshot()
	require.Greater(t, stats.rejected, 0)
	require.Len(t, stats.acceptedSeries, len(series))
	require.Less(t, sender.maxSeriesPerRequest.Load(), int64(len(series)))
	require.Equal(t, stats.requests, stats.rejected+stats.accepted)
}

func TestDatadog413LimitSurvivesLaterFailure(t *testing.T) {
	series := catalystMetricSeriesSized(10, 0, 0)
	submitter := &rejectThenFailSubmitter{}
	sender := &Datadog{
		compress:      true,
		payloadLimits: defaultDatadogPayloadLimits(),
		submitter:     submitter,
	}
	sender.maxSeriesPerRequest.Store(int64(len(series)))

	err := sender.sendAPI(context.Background(), series, catalystMetricsMetadata())
	require.ErrorContains(t, err, "network failure")
	require.Equal(t, 2, submitter.calls)
	require.Equal(t, int64(len(series)/2), sender.maxSeriesPerRequest.Load())
}

func TestPrepareDatadogPayloadStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	series := catalystMetricSeriesSized(10, 0, 0)
	_, _, err := prepareDatadogPayload(ctx, series, 0, 10, true, defaultDatadogPayloadLimits())
	require.ErrorIs(t, err, context.Canceled)
}
