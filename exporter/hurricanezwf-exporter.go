package exporter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

var _ prometheus.Gatherer = (*Gather)(nil)

type Gather struct {
	ctx                    context.Context
	client                 *mongo.Client
	exporter               *CustomizedExporter
	disableDefaultRegistry bool
	scrapeTimeoutSeconds   int64
}

func NewGather(ctx context.Context, client *mongo.Client, exporter *CustomizedExporter, disableDefaultRegistry bool, scrapeTimeoutSeconds int64) *Gather {
	return &Gather{
		ctx:                    ctx,
		client:                 client,
		exporter:               exporter,
		disableDefaultRegistry: disableDefaultRegistry,
		scrapeTimeoutSeconds:   scrapeTimeoutSeconds,
	}
}

func (g *Gather) Gather() ([]*dto.MetricFamily, error) {
	var metrics []*dto.MetricFamily

	if !g.disableDefaultRegistry {
		mf, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			return nil, fmt.Errorf("failed to gather default golang metrics, %w", err)
		}
		metrics = append(metrics, mf...)
	}

	mf, err := g.exporter.Gather(g.ctx, g.client, g.scrapeTimeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to gather mongodb metrics, %w", err)
	}
	metrics = append(metrics, mf...)

	return metrics, nil
}

func (g *Gather) Close() {
	if g.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := g.client.Disconnect(ctx); err != nil {
			g.exporter.logger.Error("failed to disconnect mongo client", err)
		}
	}
}

// CustomizedExporter holds Exporter methods and attributes.
type CustomizedExporter struct {
	client *mongo.Client
	logger *logrus.Logger
	opts   *Opts

	lock                  *sync.Mutex
	totalCollectionsCount int
}

// NewCustomizedExporter to the database and returns a new Exporter instance.
func NewCustomizedExporter(client *mongo.Client, opts *Opts) *CustomizedExporter {
	if opts == nil {
		opts = new(Opts)
	}

	if opts.Logger == nil {
		opts.Logger = logrus.New()
	}

	return &CustomizedExporter{
		client: client,
		logger: opts.Logger,
		opts:   opts,

		lock:                  &sync.Mutex{},
		totalCollectionsCount: -1, // Not calculated yet. waiting the db connection.
	}
}

func (e *CustomizedExporter) getTotalCollectionsCount() int {
	e.lock.Lock()
	defer e.lock.Unlock()

	return e.totalCollectionsCount
}

func (e *CustomizedExporter) Gather(ctx context.Context, client *mongo.Client, scrapeTimeoutSeconds int64) ([]*dto.MetricFamily, error) {
	if scrapeTimeoutSeconds <= 0 {
		scrapeTimeoutSeconds = 10
	}
	scrapeTimeoutSeconds -= int64(e.opts.TimeoutOffset)

	ctx, cancel := context.WithTimeout(ctx, time.Duration(scrapeTimeoutSeconds)*time.Second)
	defer cancel()

	if client != nil && e.getTotalCollectionsCount() <= 0 {
		count, err := nonSystemCollectionsCount(ctx, client, nil, nil)
		if err == nil {
			e.lock.Lock()
			e.totalCollectionsCount = count
			e.lock.Unlock()
		}
	}

	var ti *topologyInfo
	if client != nil {
		// Topology can change between requests, so we need to get it every time.
		ti = newTopologyInfo(ctx, client, e.logger)
	}

	return e.makeRegistry(ctx, client, ti, *e.opts).Gather()
}

func (e *CustomizedExporter) makeRegistry(ctx context.Context, client *mongo.Client, topologyInfo labelsGetter, requestOpts Opts) *prometheus.Registry {
	registry := prometheus.NewRegistry()

	nodeType, err := getNodeType(ctx, client)
	if err != nil {
		e.logger.Errorf("Registry - Cannot get node type to check if this is a mongos : %s", err)
	}

	gc := newGeneralCollector(ctx, client, nodeType, e.opts.Logger)
	registry.MustRegister(gc)

	if client == nil {
		return registry
	}

	// Enable collectors like collstats and indexstats depending on the number of collections
	// present in the database.
	limitsOk := false
	if e.opts.CollStatsLimit <= 0 || // Unlimited
		e.getTotalCollectionsCount() <= e.opts.CollStatsLimit {
		limitsOk = true
	}

	if e.opts.CollectAll {
		if len(e.opts.CollStatsNamespaces) == 0 {
			e.opts.DiscoveringMode = true
		}
		e.opts.EnableDiagnosticData = true
		e.opts.EnableDBStats = true
		e.opts.EnableDBStatsFreeStorage = true
		e.opts.EnableCollStats = true
		e.opts.EnableTopMetrics = true
		e.opts.EnableReplicasetStatus = true
		e.opts.EnableIndexStats = true
		e.opts.EnableCurrentopMetrics = true
		e.opts.EnableProfile = true
		e.opts.EnableShards = true
		e.opts.EnableFCV = true
		e.opts.EnablePBMMetrics = true
	}

	// arbiter only have isMaster privileges
	if nodeType == typeArbiter {
		e.opts.EnableDBStats = false
		e.opts.EnableDBStatsFreeStorage = false
		e.opts.EnableCollStats = false
		e.opts.EnableTopMetrics = false
		e.opts.EnableReplicasetStatus = false
		e.opts.EnableIndexStats = false
		e.opts.EnableCurrentopMetrics = false
		e.opts.EnableProfile = false
		e.opts.EnableShards = false
		e.opts.EnableFCV = false
		e.opts.EnablePBMMetrics = false
	}

	// If we manually set the collection names we want or auto discovery is set.
	if (len(e.opts.CollStatsNamespaces) > 0 || e.opts.DiscoveringMode) && e.opts.EnableCollStats && limitsOk && requestOpts.EnableCollStats {
		cc := newCollectionStatsCollector(ctx, client, e.opts.Logger,
			e.opts.CompatibleMode, e.opts.DiscoveringMode,
			topologyInfo, e.opts.CollStatsNamespaces)
		registry.MustRegister(cc)
	}

	// If we manually set the collection names we want or auto discovery is set.
	if (len(e.opts.IndexStatsCollections) > 0 || e.opts.DiscoveringMode) && e.opts.EnableIndexStats && limitsOk && requestOpts.EnableIndexStats {
		ic := newIndexStatsCollector(ctx, client, e.opts.Logger,
			e.opts.DiscoveringMode, e.opts.EnableOverrideDescendingIndex,
			topologyInfo, e.opts.IndexStatsCollections)
		registry.MustRegister(ic)
	}

	if e.opts.EnableDiagnosticData && requestOpts.EnableDiagnosticData {
		ddc := newDiagnosticDataCollector(ctx, client, e.opts.Logger,
			e.opts.CompatibleMode, topologyInfo)
		registry.MustRegister(ddc)
	}

	if e.opts.EnableDBStats && limitsOk && requestOpts.EnableDBStats {
		cc := newDBStatsCollector(ctx, client, e.opts.Logger,
			e.opts.CompatibleMode, topologyInfo, nil, e.opts.EnableDBStatsFreeStorage)
		registry.MustRegister(cc)
	}

	if e.opts.EnableCurrentopMetrics && nodeType != typeMongos && limitsOk && requestOpts.EnableCurrentopMetrics && e.opts.CurrentOpSlowTime != "" {
		coc := newCurrentopCollector(ctx, client, e.opts.Logger,
			e.opts.CompatibleMode, topologyInfo, e.opts.CurrentOpSlowTime)
		registry.MustRegister(coc)
	}

	if e.opts.EnableProfile && nodeType != typeMongos && limitsOk && requestOpts.EnableProfile && e.opts.ProfileTimeTS != 0 {
		pc := newProfileCollector(ctx, client, e.opts.Logger,
			e.opts.CompatibleMode, topologyInfo, e.opts.ProfileTimeTS)
		registry.MustRegister(pc)
	}

	if e.opts.EnableTopMetrics && nodeType != typeMongos && limitsOk && requestOpts.EnableTopMetrics {
		tc := newTopCollector(ctx, client, e.opts.Logger,
			e.opts.CompatibleMode, topologyInfo)
		registry.MustRegister(tc)
	}

	// replSetGetStatus is not supported through mongos.
	if e.opts.EnableReplicasetStatus && nodeType != typeMongos && requestOpts.EnableReplicasetStatus {
		rsgsc := newReplicationSetStatusCollector(ctx, client, e.opts.Logger,
			e.opts.CompatibleMode, topologyInfo)
		registry.MustRegister(rsgsc)
	}

	if e.opts.EnableShards && nodeType == typeMongos && requestOpts.EnableShards {
		sc := newShardsCollector(ctx, client, e.opts.Logger, e.opts.CompatibleMode)
		registry.MustRegister(sc)
	}

	if e.opts.EnableFCV && nodeType != typeMongos {
		fcvc := newFeatureCompatibilityCollector(ctx, client, e.opts.Logger)
		registry.MustRegister(fcvc)
	}

	if e.opts.EnablePBMMetrics && requestOpts.EnablePBMMetrics {
		pbmc := newPbmCollector(ctx, client, e.opts.URI, e.opts.Logger)
		registry.MustRegister(pbmc)
	}

	return registry
}
