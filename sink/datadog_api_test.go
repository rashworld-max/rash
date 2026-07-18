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
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/stretchr/testify/require"

	"github.com/cashapp/blip"
)

type rejectThenFailSubmitter struct {
	calls int
}

type checkpointFailureSubmitter struct {
	calls         int
	acceptedNames map[string]int
	acceptedBody  map[string]int
}

func (s *checkpointFailureSubmitter) Submit(_ context.Context, payload preparedDatadogPayload) (datadogSubmitResult, error) {
	s.calls++
	if s.calls == 2 {
		return datadogSubmitResult{}, errors.New("injected checkpoint failure")
	}

	var decoded datadogV2.MetricPayload
	if err := json.Unmarshal(payload.body, &decoded); err != nil {
		return datadogSubmitResult{}, err
	}
	if s.acceptedNames == nil {
		s.acceptedNames = map[string]int{}
		s.acceptedBody = map[string]int{}
	}
	for _, series := range decoded.Series {
		s.acceptedNames[series.Metric]++
	}
	s.acceptedBody[string(payload.body)]++
	return datadogSubmitResult{statusCode: http.StatusAccepted}, nil
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

func TestDatadogRetryCheckpointDoesNotResendAcknowledgedPayload(t *testing.T) {
	const metricCount = 40
	submitter := &checkpointFailureSubmitter{}
	sender := &Datadog{
		monitorId: "checkpoint-test",
		compress:  false,
		payloadLimits: datadogPayloadLimits{
			maxCompressed:      1_500,
			maxDecompressed:    10_000,
			targetCompressed:   1_200,
			targetDecompressed: 9_000,
			maxSeries:          metricCount,
		},
		submitter: submitter,
	}
	sender.maxSeriesPerRequest.Store(metricCount)
	retry := NewRetry(RetryArgs{
		MonitorId:     "checkpoint-test",
		Sink:          sender,
		BufferSize:    2,
		SendTimeout:   5 * time.Second,
		SendRetryWait: time.Millisecond,
	})

	require.NoError(t, retry.Send(context.Background(), getBlipMetrics(metricCount, blip.GAUGE, 1, false)))
	require.Greater(t, submitter.calls, 3, "test data must span at least three chunks")
	require.Equal(t, metricCount, len(submitter.acceptedNames))
	for name, count := range submitter.acceptedNames {
		require.Equalf(t, 1, count, "acknowledged metric %s was submitted more than once", name)
	}
	for body, count := range submitter.acceptedBody {
		require.Equalf(t, 1, count, "acknowledged payload was submitted more than once: %s", body)
	}
	require.Equal(t, -1, retry.top, "successful resume should remove the checkpointed queue entry")
}
