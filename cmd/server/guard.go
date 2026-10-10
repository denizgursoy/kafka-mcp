package main

import (
	"crypto/subtle"
	"net/http"
	"slices"
	"strings"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// guard protects the MCP routes from callers the operator did not allow.
//
// CORS alone does not: it only decides whether a browser lets the page read
// the response, and ada forwards a request from a refused origin to the
// handler anyway. A POST that runs delete_topic does its damage whether or not
// the page sees the answer, so a request from an origin that is not allowed is
// refused here, before any tool runs.
//
// Requests without an Origin header are command-line clients, not pages, and
// are left to the token check.
func guard(settings config.HTTP, next http.Handler) http.Handler {
	allowAny := slices.Contains(settings.CORS.AllowOrigins, "*")
	token := []byte(settings.AuthToken)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !allowAny && !originAllowed(origin, settings.CORS.AllowOrigins) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}

		// A browser sends no credentials on a preflight, so requiring the
		// token there would block every browser client. A preflight runs no
		// tool; the request it precedes is checked below.
		preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""

		if len(token) > 0 && !preflight {
			presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(presented), token) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="kafka-mcp"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// originAllowed matches an origin against the configured list, using the same
// '*' and '?' wildcards the CORS middleware accepts.
func originAllowed(origin string, allowed []string) bool {
	for _, pattern := range allowed {
		if pattern == origin || wildcardMatch(pattern, origin) {
			return true
		}
	}

	return false
}

func wildcardMatch(pattern, value string) bool {
	if !strings.ContainsAny(pattern, "*?") {
		return false
	}

	// Iterative glob matching with backtracking on the last '*'.
	p, v, star, mark := 0, 0, -1, 0
	for v < len(value) {
		switch {
		case p < len(pattern) && (pattern[p] == '?' || pattern[p] == value[v]):
			p++
			v++
		case p < len(pattern) && pattern[p] == '*':
			star, mark = p, v
			p++
		case star >= 0:
			p = star + 1
			mark++
			v = mark
		default:
			return false
		}
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}

	return p == len(pattern)
}
