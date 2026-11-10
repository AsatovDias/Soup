package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "errors"
    "io"
    "log"
    "net/http"
    "strconv"
    "strings"
    "time"
)

func routes(db *sql.DB) http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
        if err := db.PingContext(r.Context()); err != nil {
            respond(w, 503, map[string]string{"error": "database unavailable"})
            return
        }
        respond(w, 200, map[string]string{"status": "ok"})
    })
    mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
        key := r.Header.Get("Idempotency-Key")
        if key == "" || len(key) > 128 || strings.TrimSpace(key) != key {
            respond(w, 400, map[string]string{"error": "invalid Idempotency-Key"})
            return
        }
        decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
        decoder.DisallowUnknownFields()
        var p payload
        if err := decoder.Decode(&p); err != nil || p.DurationMS < 1 || p.DurationMS > 1000 {
            respond(w, 400, map[string]string{"error": "expected duration_ms in [1,1000] and optional fail boolean"})
            return
        }
        if err := decoder.Decode(new(any)); err != io.EOF {
            respond(w, 400, map[string]string{"error": "expected a single JSON object"})
            return
        }
        j, err := readJob(db.QueryRowContext(r.Context(), `
            INSERT INTO jobs(request_key,duration_ms,should_fail) VALUES ($1,$2,$3)
            ON CONFLICT (request_key) DO NOTHING RETURNING `+jobColumns, key, p.DurationMS, p.Fail))
        if err == nil {
            respond(w, 201, j)
            return
        }
        if err != sql.ErrNoRows {
            jobResponse(w, j, err)
            return
        }
        // A separate READ COMMITTED statement sees the winner of a concurrent insert.
        j, err = readJob(db.QueryRowContext(r.Context(), "SELECT "+jobColumns+" FROM jobs WHERE request_key=$1", key))
        if err == nil && j.Payload != p {
            respond(w, 409, map[string]string{"error": "key already used with a different payload"})
            return
        }
        jobResponse(w, j, err)
    })
    mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
        id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
        if err != nil || id < 1 {
            respond(w, 400, map[string]string{"error": "invalid job ID"})
            return
        }
        j, err := readJob(db.QueryRowContext(r.Context(), "SELECT "+jobColumns+" FROM jobs WHERE id=$1", id))
        jobResponse(w, j, err)
    })
    mux.HandleFunc("POST /jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
        id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
        if err != nil || id < 1 {
            respond(w, 400, map[string]string{"error": "invalid job ID"})
            return
        }
        j, err := cancelJob(r.Context(), db, id)
        jobResponse(w, j, err)
    })
    mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
        limit, after := int64(50), int64(0)
        var err error
        if r.URL.Query().Has("limit") {
            limit, err = strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
        }
        if err != nil || limit < 1 || limit > 100 {
            respond(w, 400, map[string]string{"error": "limit must be in [1,100]"})
            return
        }
        if r.URL.Query().Has("after") {
            after, err = strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
        }
        if err != nil || after < 0 {
            respond(w, 400, map[string]string{"error": "invalid after cursor"})
            return
        }
        rows, err := db.QueryContext(r.Context(), "SELECT "+jobColumns+" FROM jobs WHERE id>$1 ORDER BY id LIMIT $2", after, limit+1)
        if err != nil {
            internalError(w, err)
            return
        }
        defer rows.Close()
        items := make([]job, 0, limit+1)
        for rows.Next() {
            j, err := readJob(rows)
            if err != nil {
                internalError(w, err)
                return
            }
            items = append(items, j)
        }
        if err := rows.Err(); err != nil {
            internalError(w, err)
            return
        }
        var next *int64
        if int64(len(items)) > limit {
            items = items[:limit]
            next = &items[len(items)-1].ID
        }
        respond(w, 200, struct {
            Items []job `json:"items"`
            Next *int64 `json:"next_after"`
        }{items, next})
    })
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
        defer cancel()
        mux.ServeHTTP(w, r.WithContext(ctx))
    })
}

func respond(w http.ResponseWriter, status int, body any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    _ = json.NewEncoder(w).Encode(body)
}

func jobResponse(w http.ResponseWriter, j job, err error) {
    switch {
    case err == nil:
        respond(w, 200, j)
    case errors.Is(err, sql.ErrNoRows):
        respond(w, 404, map[string]string{"error": "job not found"})
    default:
        internalError(w, err)
    }
}

func internalError(w http.ResponseWriter, err error) {
    log.Printf("api database error: %v", err)
    respond(w, 500, map[string]string{"error": "internal error"})
}
