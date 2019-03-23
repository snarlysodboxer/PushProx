package util

import (
	"errors"
	"net/http"
	"strconv"
	"time"
)

func GetScrapeTimeout(maxScrapeTimeout, defaultScrapeTimeout *time.Duration, h http.Header) time.Duration {
	timeout := *defaultScrapeTimeout
	headerTimeout, err := GetHeaderTimeout(h)
	if err == nil {
		timeout = headerTimeout
	}
	if timeout > *maxScrapeTimeout {
		timeout = *maxScrapeTimeout
	}
	return timeout
}

func GetHeaderTimeout(h http.Header) (time.Duration, error) {
	header := h.Get("X-Prometheus-Scrape-Timeout-Seconds")
	if header == "" {
		return time.Duration(0 * time.Second), errors.New("X-Prometheus-Scrape-Timeout-Seconds header is not set")
	}
	timeoutSeconds, err := strconv.ParseFloat(header, 64)
	if err != nil {
		return time.Duration(0 * time.Second), err
	}

	return time.Duration(timeoutSeconds * 1e9), nil
}
