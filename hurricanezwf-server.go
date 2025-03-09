package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/percona/mongodb_exporter/exporter"
	"github.com/percona/mongodb_exporter/exporter/dsn_fix"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
)

func HTTPListenAndServ(flags GlobalFlags, log *logrus.Logger) {
	http.HandleFunc(flags.WebTelemetryPath, handleProbe(log, flags))

	log.Info("starting mongo-exporter ...")
	go func(addr string) {
		time.Sleep(1 * time.Second)
		log.Info("mongo-exporter started at ", addr)
	}(flags.WebListenAddress)

	server := &http.Server{
		Addr: flags.WebListenAddress,
	}
	if err := server.ListenAndServe(); err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		log.Fatalf("error starting server: %v", err)
	}
}

func handleProbe(log *logrus.Logger, flags GlobalFlags) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// 从请求头解析抓取超时;
		seconds, err := strconv.ParseInt(r.Header.Get("X-Prometheus-Scrape-Timeout-Seconds"), 10, 64)
		// To support also older ones vmagents.
		if err != nil {
			seconds = 10
		}

		// 从 query 参数中解析探测目标;
		params := r.URL.Query()
		target := params.Get("target")
		if target == "" {
			http.Error(w, "target is required", http.StatusBadRequest)
			return
		}

		_, err = dsn_fix.ClientOptionsForDSN(target)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid target value `%s`, %v", target, err), http.StatusBadRequest)
			return
		}
		// Note: 如果没有用户名和密码, 则使用默认账户;
		uri := buildURI(target, flags.User, flags.Password)
		log.Debug("probe target `", uri, "`")

		exporterOpts := expoterOptsFrom(flags, log, uri)
		mongocli, err := newMongoClient(r.Context(), exporterOpts)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to connect to MongoDB: %v", err), http.StatusInternalServerError)
			return
		}
		defer func() {
			if err := mongocli.Disconnect(r.Context()); err != nil {
				log.Error("failed to disconnect MongoDB client", err)
			}
		}()

		exp := exporter.NewCustomizedExporter(mongocli, exporterOpts)
		gather := exporter.NewGather(r.Context(), mongocli, exp, !flags.EnableExporterMetrics, seconds)
		h := promhttp.HandlerFor(gather, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	}
}

func newMongoClient(ctx context.Context, opts *exporter.Opts) (*mongo.Client, error) {
	clientOpts, err := dsn_fix.ClientOptionsForDSN(opts.URI)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %w", err)
	}

	clientOpts.SetDirect(opts.DirectConnect)
	clientOpts.SetAppName("mongodb_exporter")

	clientOpts.SetMinPoolSize(0)               // 最小连接数
	clientOpts.SetMaxPoolSize(5)               // 最大连接数
	clientOpts.SetMaxConnIdleTime(time.Minute) // 连接最大空闲时间

	if clientOpts.ConnectTimeout == nil {
		connectTimeout := time.Duration(opts.ConnectTimeoutMS) * time.Millisecond
		clientOpts.SetConnectTimeout(connectTimeout)
		clientOpts.SetServerSelectionTimeout(connectTimeout)
	}

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("invalid MongoDB options: %w", err)
	}

	if err = client.Ping(ctx, nil); err != nil {
		// Ping failed. Close background connections. Error is ignored since the ping error is more relevant.
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("cannot connect to MongoDB: %w", err)
	}

	return client, nil
}

func expoterOptsFrom(opts GlobalFlags, log *logrus.Logger, uri string) *exporter.Opts {
	return &exporter.Opts{
		CollStatsNamespaces:   strings.Split(opts.CollStatsNamespaces, ","),
		CompatibleMode:        opts.CompatibleMode,
		DiscoveringMode:       opts.DiscoveringMode,
		IndexStatsCollections: strings.Split(opts.IndexStatsCollections, ","),
		Logger:                log,
		URI:                   uri,
		GlobalConnPool:        opts.GlobalConnPool,
		DirectConnect:         opts.DirectConnect,
		ConnectTimeoutMS:      opts.ConnectTimeoutMS,
		TimeoutOffset:         opts.TimeoutOffset,

		DisableDefaultRegistry:   !opts.EnableExporterMetrics,
		EnableDiagnosticData:     opts.EnableDiagnosticData,
		EnableReplicasetStatus:   opts.EnableReplicasetStatus,
		EnableCurrentopMetrics:   opts.EnableCurrentopMetrics,
		EnableTopMetrics:         opts.EnableTopMetrics,
		EnableDBStats:            opts.EnableDBStats,
		EnableDBStatsFreeStorage: opts.EnableDBStatsFreeStorage,
		EnableIndexStats:         opts.EnableIndexStats,
		EnableCollStats:          opts.EnableCollStats,
		EnableProfile:            opts.EnableProfile,
		EnableShards:             opts.EnableShards,
		EnableFCV:                opts.EnableFCV,
		EnablePBMMetrics:         opts.EnablePBM,

		EnableOverrideDescendingIndex: opts.EnableOverrideDescendingIndex,

		CollStatsLimit:    opts.CollStatsLimit,
		CollectAll:        opts.CollectAll,
		ProfileTimeTS:     opts.ProfileTimeTS,
		CurrentOpSlowTime: opts.CurrentOpSlowTime,
	}
}
