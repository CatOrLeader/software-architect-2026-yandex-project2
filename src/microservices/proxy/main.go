package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const upstreamHeader = "X-CinemaAbyss-Upstream"

type config struct {
	port                   string
	monolithURL            *url.URL
	moviesServiceURL       *url.URL
	eventsServiceURL       *url.URL
	gradualMigration       bool
	moviesMigrationPercent int
}

type gateway struct {
	monolithProxy          http.Handler
	moviesProxy            http.Handler
	eventsProxy            http.Handler
	gradualMigration       bool
	moviesMigrationPercent int
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	handler := newGateway(cfg)
	server := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf(
		"starting proxy on port %s (gradual_migration=%t, movies_migration_percent=%d)",
		cfg.port,
		cfg.gradualMigration,
		cfg.moviesMigrationPercent,
	)
	log.Fatal(server.ListenAndServe())
}

func loadConfig() (config, error) {
	var cfg config
	cfg.port = envOrDefault("PORT", "8000")

	var err error
	if cfg.monolithURL, err = parseServiceURL("MONOLITH_URL", "http://localhost:8080"); err != nil {
		return config{}, err
	}
	if cfg.moviesServiceURL, err = parseServiceURL("MOVIES_SERVICE_URL", "http://localhost:8081"); err != nil {
		return config{}, err
	}
	if cfg.eventsServiceURL, err = parseServiceURL("EVENTS_SERVICE_URL", "http://localhost:8082"); err != nil {
		return config{}, err
	}

	cfg.gradualMigration, err = strconv.ParseBool(envOrDefault("GRADUAL_MIGRATION", "true"))
	if err != nil {
		return config{}, fmt.Errorf("GRADUAL_MIGRATION must be a boolean: %w", err)
	}

	cfg.moviesMigrationPercent, err = strconv.Atoi(envOrDefault("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil {
		return config{}, fmt.Errorf("MOVIES_MIGRATION_PERCENT must be an integer: %w", err)
	}
	if cfg.moviesMigrationPercent < 0 || cfg.moviesMigrationPercent > 100 {
		return config{}, errors.New("MOVIES_MIGRATION_PERCENT must be between 0 and 100")
	}

	return cfg, nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func parseServiceURL(name, fallback string) (*url.URL, error) {
	rawURL := envOrDefault(name, fallback)
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%s is invalid: %w", name, err)
	}
	if (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return nil, fmt.Errorf("%s must be an absolute HTTP(S) URL", name)
	}
	return parsedURL, nil
}

func newGateway(cfg config) *gateway {
	return &gateway{
		monolithProxy:          newUpstreamProxy("monolith", cfg.monolithURL),
		moviesProxy:            newUpstreamProxy("movies-service", cfg.moviesServiceURL),
		eventsProxy:            newUpstreamProxy("events-service", cfg.eventsServiceURL),
		gradualMigration:       cfg.gradualMigration,
		moviesMigrationPercent: cfg.moviesMigrationPercent,
	}
}

func newUpstreamProxy(name string, target *url.URL) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           netDialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		response.Header.Set(upstreamHeader, name)
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("upstream %s failed for %s %s: %v", name, r.Method, r.URL.RequestURI(), err)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(upstreamHeader, name)
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "upstream service unavailable"})
	}
	return proxy
}

var netDialer = &net.Dialer{
	Timeout:   5 * time.Second,
	KeepAlive: 30 * time.Second,
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/health":
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
	case matchesPath(r.URL.Path, "/api/movies"):
		g.moviesHandler().ServeHTTP(w, r)
	case matchesPath(r.URL.Path, "/api/events"):
		g.eventsProxy.ServeHTTP(w, r)
	default:
		g.monolithProxy.ServeHTTP(w, r)
	}
}

func (g *gateway) moviesHandler() http.Handler {
	if !g.gradualMigration || g.moviesMigrationPercent == 100 {
		return g.moviesProxy
	}
	if g.moviesMigrationPercent == 0 {
		return g.monolithProxy
	}
	if rand.Intn(100) < g.moviesMigrationPercent {
		return g.moviesProxy
	}
	return g.monolithProxy
}

func matchesPath(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}
