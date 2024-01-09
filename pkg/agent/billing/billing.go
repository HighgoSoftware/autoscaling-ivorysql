package billing

import (
	"context"
	"errors"
	"math"
	"net/http"
	"time"

	"go.uber.org/zap"

	"k8s.io/apimachinery/pkg/types"

	vmapi "github.com/neondatabase/autoscaling/neonvm/apis/neonvm/v1"
	"github.com/neondatabase/autoscaling/pkg/api"
	"github.com/neondatabase/autoscaling/pkg/billing"
	"github.com/neondatabase/autoscaling/pkg/util"
)

type Config struct {
	URL                       string `json:"url"`
	CPUMetricName             string `json:"cpuMetricName"`
	ActiveTimeMetricName      string `json:"activeTimeMetricName"`
	IngressBytesMetricName    string `json:"ingressBytesMetricName"`
	EgressBytesMetricName     string `json:"egressBytesMetricName"`
	CollectEverySeconds       uint   `json:"collectEverySeconds"`
	AccumulateEverySeconds    uint   `json:"accumulateEverySeconds"`
	PushEverySeconds          uint   `json:"pushEverySeconds"`
	PushRequestTimeoutSeconds uint   `json:"pushRequestTimeoutSeconds"`
	MaxBatchSize              uint   `json:"maxBatchSize"`
}

type metricsState struct {
	historical      map[metricsKey]vmMetricsHistory
	present         map[metricsKey]vmMetricsInstant
	lastCollectTime *time.Time
	pushWindowStart time.Time
}

type metricsKey struct {
	uid        types.UID
	endpointID string
}

type vmMetricsHistory struct {
	lastSlice         *metricsTimeSlice
	total             vmMetricsSeconds
	totalIngressBytes vmapi.NetworkBytes
	totalEgressBytes  vmapi.NetworkBytes
}

type metricsTimeSlice struct {
	metrics   vmMetricsInstant
	startTime time.Time
	endTime   time.Time
}

func (m *metricsTimeSlice) Duration() time.Duration { return m.endTime.Sub(m.startTime) }

type vmMetricsInstant struct {
	// cpu stores the cpu allocation at a particular instant.
	cpu vmapi.MilliCPU
	// number of bytes received by the VM from the open internet since the last time slice
	ingressBytes vmapi.NetworkBytes
	// number of bytes sent by the VM to the open internet since the last time slice
	egressBytes vmapi.NetworkBytes
}

// vmMetricsSeconds is like vmMetrics, but the values cover the allocation over time
type vmMetricsSeconds struct {
	// cpu stores the CPU seconds allocated to the VM, roughly equivalent to the integral of CPU
	// usage over time.
	cpu float64
	// activeTime stores the total time that the VM was active
	activeTime time.Duration
}

type vmMetricsKV struct {
	key   metricsKey
	value vmMetricsInstant
}

func RunBillingMetricsCollector(
	backgroundCtx context.Context,
	parentLogger *zap.Logger,
	conf *Config,
	store VMStoreForNode,
	metrics PromMetrics,
) {
	client := billing.NewClient(conf.URL, http.DefaultClient)

	logger := parentLogger.Named("billing")

	collectTicker := time.NewTicker(time.Second * time.Duration(conf.CollectEverySeconds))
	defer collectTicker.Stop()
	// Offset by half a second, so it's a bit more deterministic.
	time.Sleep(500 * time.Millisecond)
	accumulateTicker := time.NewTicker(time.Second * time.Duration(conf.AccumulateEverySeconds))
	defer accumulateTicker.Stop()

	state := metricsState{
		historical:      make(map[metricsKey]vmMetricsHistory),
		present:         make(map[metricsKey]vmMetricsInstant),
		lastCollectTime: nil,
		pushWindowStart: time.Now(),
	}

	queueWriter, queueReader := newEventQueue[*billing.IncrementalEvent](metrics.queueSizeCurrent)

	// Start the sender
	signalDone, thisThreadFinished := util.NewCondChannelPair()
	defer signalDone.Send()
	sender := eventSender{
		client:            client,
		config:            conf,
		metrics:           metrics,
		queue:             queueReader,
		collectorFinished: thisThreadFinished,
		lastSendDuration:  0,
	}
	go sender.senderLoop(logger.Named("send"))

	// The rest of this function is to do with collection
	logger = logger.Named("collect")

	state.collect(backgroundCtx, store, metrics, logger)

	for {
		select {
		case <-collectTicker.C:
			logger.Info("Collecting billing state")
			if store.Stopped() && backgroundCtx.Err() == nil {
				err := errors.New("VM store stopped but background context is still live")
				logger.Panic("Validation check failed", zap.Error(err))
			}
			state.collect(backgroundCtx, store, metrics, logger)
		case <-accumulateTicker.C:
			logger.Info("Creating billing batch")
			state.drainEnqueue(logger, conf, client.Hostname(), queueWriter)
		case <-backgroundCtx.Done():
			return
		}
	}
}

func collectMetricsForVM(vm *vmapi.VirtualMachine, ctx context.Context, metricsChan chan vmMetricsKV) {
	byteCounts, err := vm.GetNetworkUsage(ctx)
	if err != nil {
		byteCounts = &vmapi.VirtualMachineNetworkUsage{
			IngressBytes: 0,
			EgressBytes:  0,
		}
	}
	endpointID := vm.Annotations[api.AnnotationBillingEndpointID]
	key := metricsKey{
		uid:        vm.UID,
		endpointID: endpointID,
	}

	presentMetrics := vmMetricsInstant{
		cpu:          *vm.Status.CPUs,
		ingressBytes: byteCounts.IngressBytes,
		egressBytes:  byteCounts.EgressBytes,
	}

	result := vmMetricsKV{
		key:   key,
		value: presentMetrics,
	}
	metricsChan <- result
}

func (s *metricsState) collect(ctx context.Context, store VMStoreForNode, metrics PromMetrics, logger *zap.Logger) {
	now := time.Now()

	metricsBatch := metrics.forBatch()
	defer metricsBatch.finish() // This doesn't *really* need to be deferred, but it's up here so we don't forget

	old := s.present
	s.present = make(map[metricsKey]vmMetricsInstant)
	var vmsOnThisNode []*vmapi.VirtualMachine
	if store.Failing() {
		logger.Error("VM store is currently stopped. No events will be recorded")
	} else {
		vmsOnThisNode = store.ListIndexed(func(i *VMNodeIndex) []*vmapi.VirtualMachine {
			return i.List()
		})
	}

	metricsChan := make(chan vmMetricsKV, len(vmsOnThisNode))
	metricsToCollect := 0
	for _, vm := range vmsOnThisNode {
		_, isEndpoint := vm.Annotations[api.AnnotationBillingEndpointID]
		metricsBatch.inc(isEndpointFlag(isEndpoint), autoscalingEnabledFlag(api.HasAutoscalingEnabled(vm)), vm.Status.Phase)
		if !isEndpoint {
			// we're only reporting metrics for VMs with endpoint IDs, and this VM doesn't have one
			continue
		}

		if !vm.Status.Phase.IsAlive() || vm.Status.CPUs == nil {
			continue
		}

		go collectMetricsForVM(vm, ctx, metricsChan)
		metricsToCollect += 1
	}

	for i := 0; i < metricsToCollect; i++ {
		kv := <-metricsChan
		key := kv.key
		presentMetrics := kv.value

		if oldMetrics, ok := old[key]; ok {
			// The VM was present from s.lastTime to now. Add a time slice to its metrics history.
			timeSlice := metricsTimeSlice{
				metrics: vmMetricsInstant{
					// strategically under-bill by assigning the minimum to the entire time slice.
					cpu:          util.Min(oldMetrics.cpu, presentMetrics.cpu),
					ingressBytes: presentMetrics.ingressBytes - oldMetrics.ingressBytes,
					egressBytes:  presentMetrics.egressBytes - oldMetrics.egressBytes,
				},
				// note: we know s.lastTime != nil because otherwise old would be empty.
				startTime: *s.lastCollectTime,
				endTime:   now,
			}

			vmHistory, ok := s.historical[key]
			if !ok {
				vmHistory = vmMetricsHistory{
					lastSlice:         nil,
					total:             vmMetricsSeconds{cpu: 0, activeTime: time.Duration(0)},
					totalIngressBytes: 0,
					totalEgressBytes:  0,
				}
			}
			// append the slice, merging with the previous if the resource usage was the same
			vmHistory.appendSlice(timeSlice)
			s.historical[key] = vmHistory
		}

		s.present[key] = presentMetrics
	}

	s.lastCollectTime = &now
}

func (h *vmMetricsHistory) appendSlice(timeSlice metricsTimeSlice) {
	// Try to extend the existing period of continuous usage
	if h.lastSlice != nil && h.lastSlice.tryMerge(timeSlice) {
		return
	}

	// Something's new. Push previous time slice, start new one:
	h.finalizeCurrentTimeSlice()
	h.lastSlice = &timeSlice
}

// finalizeCurrentTimeSlice pushes the current time slice onto h.total
//
// This ends up rounding down the total time spent on a given time slice, so it's best to defer
// calling this function until it's actually needed.
func (h *vmMetricsHistory) finalizeCurrentTimeSlice() {
	if h.lastSlice == nil {
		return
	}

	duration := h.lastSlice.Duration()
	if duration < 0 {
		panic("negative duration")
	}

	// TODO: This approach is imperfect. Floating-point math is probably *fine*, but really not
	// something we want to rely on. A "proper" solution is a lot of work, but long-term valuable.
	metricsSeconds := vmMetricsSeconds{
		cpu:        duration.Seconds() * h.lastSlice.metrics.cpu.AsFloat64(),
		activeTime: duration,
	}
	h.total.cpu += metricsSeconds.cpu
	h.total.activeTime += metricsSeconds.activeTime
	h.totalIngressBytes += h.lastSlice.metrics.ingressBytes
	h.totalEgressBytes += h.lastSlice.metrics.egressBytes

	h.lastSlice = nil
}

// tryMerge attempts to merge s and next (assuming that next is after s), returning true only if
// that merging was successful.
//
// Merging may fail if s.endTime != next.startTime or s.metrics != next.metrics.
func (s *metricsTimeSlice) tryMerge(next metricsTimeSlice) bool {
	merged := s.endTime == next.startTime && s.metrics == next.metrics
	if merged {
		s.endTime = next.endTime
	}
	return merged
}

func logAddedEvent(logger *zap.Logger, event *billing.IncrementalEvent) *billing.IncrementalEvent {
	logger.Info(
		"Adding event to batch",
		zap.String("IdempotencyKey", event.IdempotencyKey),
		zap.String("EndpointID", event.EndpointID),
		zap.String("MetricName", event.MetricName),
		zap.Int("Value", event.Value),
	)
	return event
}

// drainEnqueue clears the current history, adding it as events to the queue
func (s *metricsState) drainEnqueue(logger *zap.Logger, conf *Config, hostname string, queue eventQueuePusher[*billing.IncrementalEvent]) {
	now := time.Now()

	countInBatch := 0
	batchSize := 2 * len(s.historical)

	for key, history := range s.historical {
		history.finalizeCurrentTimeSlice()

		countInBatch += 1
		queue.enqueue(logAddedEvent(logger, billing.Enrich(now, hostname, countInBatch, batchSize, &billing.IncrementalEvent{
			MetricName:     conf.CPUMetricName,
			Type:           "", // set by billing.Enrich
			IdempotencyKey: "", // set by billing.Enrich
			EndpointID:     key.endpointID,
			// TODO: maybe we should store start/stop time in the vmMetricsHistory object itself?
			// That way we can be aligned to collection, rather than pushing.
			StartTime: s.pushWindowStart,
			StopTime:  now,
			Value:     int(math.Round(history.total.cpu)),
		})))
		countInBatch += 1
		queue.enqueue(logAddedEvent(logger, billing.Enrich(now, hostname, countInBatch, batchSize, &billing.IncrementalEvent{
			MetricName:     conf.ActiveTimeMetricName,
			Type:           "", // set by billing.Enrich
			IdempotencyKey: "", // set by billing.Enrich
			EndpointID:     key.endpointID,
			StartTime:      s.pushWindowStart,
			StopTime:       now,
			Value:          int(math.Round(history.total.activeTime.Seconds())),
		})))
		countInBatch += 1
		queue.enqueue(logAddedEvent(logger, billing.Enrich(now, hostname, countInBatch, batchSize, &billing.IncrementalEvent{
			MetricName:     conf.IngressBytesMetricName,
			Type:           "", // set by billing.Enrich
			IdempotencyKey: "", // set by billing.Enrich
			EndpointID:     key.endpointID,
			StartTime:      s.pushWindowStart,
			StopTime:       now,
			Value:          int(history.totalIngressBytes),
		})))
		countInBatch += 1
		queue.enqueue(logAddedEvent(logger, billing.Enrich(now, hostname, countInBatch, batchSize, &billing.IncrementalEvent{
			MetricName:     conf.EgressBytesMetricName,
			Type:           "", // set by billing.Enrich
			IdempotencyKey: "", // set by billing.Enrich
			EndpointID:     key.endpointID,
			StartTime:      s.pushWindowStart,
			StopTime:       now,
			Value:          int(history.totalEgressBytes),
		})))
	}

	s.pushWindowStart = now
	s.historical = make(map[metricsKey]vmMetricsHistory)
}
