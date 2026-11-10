package main

import (
    "context"
    "database/sql"
    _ "embed"
    "errors"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/lib/pq"
)

//go:embed schema.sql
var schema string

func openDB() (*sql.DB, error) {
    dsn := os.Getenv("DATABASE_URL")
    if dsn == "" {
        return nil, errors.New("DATABASE_URL is required")
    }
    connector, err := pq.NewConnector(dsn)
    if err != nil {
        return nil, err
    }
    db := sql.OpenDB(connector)
    db.SetMaxOpenConns(16)
    db.SetMaxIdleConns(16)
    return db, nil
}

func run() error {
    if len(os.Args) != 2 {
        return errors.New("usage: queue api|worker|migrate|probe")
    }
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    db, err := openDB()
    if err != nil {
        return err
    }
    defer db.Close()
    switch os.Args[1] {
    case "migrate":
        ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
        defer cancel()
        _, err := db.ExecContext(ctx, schema)
        return err
    case "api":
        return serve(ctx, db)
    case "worker":
        return worker(ctx, db)
    case "probe":
        ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
        defer cancel()
        return probe(ctx, db)
    default:
        return fmt.Errorf("unknown command %q", os.Args[1])
    }
}

func main() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
    if err := run(); err != nil {
        log.Fatal(err)
    }
}

func pause(ctx context.Context, d time.Duration) bool {
    timer := time.NewTimer(d)
    defer timer.Stop()
    select {
    case <-ctx.Done():
        return false
    case <-timer.C:
        return true
    }
}

func worker(ctx context.Context, db *sql.DB) error {
    for ctx.Err() == nil {
        step, cancel := context.WithTimeout(ctx, 3*time.Second)
        j, err := claim(step, db, 5*time.Second)
        cancel()
        if err != nil {
            if err != sql.ErrNoRows && ctx.Err() == nil {
                log.Printf("claim: %v", err)
            }
            pause(ctx, 100*time.Millisecond)
            continue
        }
        if !pause(ctx, time.Duration(j.Payload.DurationMS)*time.Millisecond) {
            break
        }
        step, cancel = context.WithTimeout(ctx, 3*time.Second)
        _, err = finish(step, db, j)
        cancel()
        if err != nil && ctx.Err() == nil {
            log.Printf("finish job %d attempt %d: %v", j.ID, j.Attempt, err)
        }
    }
    return nil
}

func serve(ctx context.Context, db *sql.DB) error {
    server := &http.Server{
        Addr: ":8080", Handler: routes(db),
        ReadHeaderTimeout: 3*time.Second, ReadTimeout: 5*time.Second,
        WriteTimeout: 7*time.Second, IdleTimeout: 30*time.Second,
    }
    done := make(chan struct{})
    go func() {
        defer close(done)
        <-ctx.Done()
        shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := server.Shutdown(shutdown); err != nil {
            _ = server.Close()
        }
    }()
    err := server.ListenAndServe()
    if errors.Is(err, http.ErrServerClosed) {
        <-done
        return nil
    }
    return err
}
