package main

import (
    "context"
    "database/sql"
    "errors"
    "fmt"
    "log"
    "time"
)

// Run on a fresh test database with all workers stopped.
func probe(ctx context.Context, db *sql.DB) error {
    var isolation string
    if err := db.QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
        return err
    }
    if isolation != "read committed" {
        return fmt.Errorf("expected read committed, got %s", isolation)
    }
    var existing int
    if err := db.QueryRowContext(ctx, "SELECT count(*) FROM jobs").Scan(&existing); err != nil {
        return err
    }
    if existing != 0 {
        return errors.New("probe requires an empty database and stopped workers")
    }
    log.Printf("PASS transaction isolation=%s", isolation)

    makeJob := func(key string) (int64, error) {
        var id int64
        err := db.QueryRowContext(ctx, "INSERT INTO jobs(request_key,duration_ms,should_fail) VALUES ($1,10,false) RETURNING id", key).Scan(&id)
        return id, err
    }

    id, err := makeJob("probe-zombie")
    if err != nil {
        return err
    }
    // Hold a real row lock on one connection and try a claim on another.
    lock, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer lock.Rollback()
    var lockedID int64
    if err := lock.QueryRowContext(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", id).Scan(&lockedID); err != nil {
        return err
    }
    short, cancel := context.WithTimeout(ctx, time.Second)
    _, err = claim(short, db, 5*time.Second)
    cancel()
    if !errors.Is(err, sql.ErrNoRows) {
        return fmt.Errorf("locked job was not skipped: %v", err)
    }
    if err := lock.Rollback(); err != nil {
        return err
    }
    old, err := claim(ctx, db, 5*time.Second)
    if err != nil || old.ID != id || old.Attempt != 1 {
        return fmt.Errorf("first claim: %+v, %v", old, err)
    }
    if _, err := claim(ctx, db, 5*time.Second); !errors.Is(err, sql.ErrNoRows) {
        return fmt.Errorf("second claim before expiry: %v", err)
    }
    log.Print("PASS locked row skipped and unexpired claim remains exclusive")

    // Move the stored deadline across database 'now', without relying on local sleep.
    if _, err := db.ExecContext(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
        return err
    }
    if applied, err := finish(ctx, db, old); err != nil || applied {
        return fmt.Errorf("expired completion without reclaim: applied=%v err=%v", applied, err)
    }
    current, err := claim(ctx, db, 5*time.Second)
    if err != nil || current.ID != id || current.Attempt != old.Attempt+1 {
        return fmt.Errorf("reclaim: %+v, %v", current, err)
    }
    if applied, err := finish(ctx, db, old); err != nil || applied {
        return fmt.Errorf("stale completion after reclaim: applied=%v err=%v", applied, err)
    }
    if applied, err := finish(ctx, db, current); err != nil || !applied {
        return fmt.Errorf("current completion: applied=%v err=%v", applied, err)
    }
    if err := checkEffect(ctx, db, id, current.Attempt, true); err != nil {
        return err
    }
    log.Print("PASS database-time expiry and stale attempt fencing")

    for _, cancelFirst := range []bool{true, false} {
        id, err := makeJob(fmt.Sprintf("probe-cancel-%v", cancelFirst))
        if err != nil {
            return err
        }
        j, err := claim(ctx, db, 5*time.Second)
        if err != nil || j.ID != id {
            return fmt.Errorf("claim for cancellation: %+v, %v", j, err)
        }
        if cancelFirst {
            cancelled, err := cancelJob(ctx, db, id)
            if err != nil || cancelled.State != "cancelled" {
                return fmt.Errorf("cancel first: %+v, %v", cancelled, err)
            }
        }
        if applied, err := finish(ctx, db, j); err != nil || applied == cancelFirst {
            return fmt.Errorf("completion ordering: cancel_first=%v applied=%v err=%v", cancelFirst, applied, err)
        }
        terminal, err := cancelJob(ctx, db, id)
        want := "succeeded"
        if cancelFirst {
            want = "cancelled"
        }
        if err != nil || terminal.State != want {
            return fmt.Errorf("cancel after completion: %+v, expected %s, err=%v", terminal, want, err)
        }
        if err := checkEffect(ctx, db, id, j.Attempt, !cancelFirst); err != nil {
            return err
        }
        log.Printf("PASS cancel/complete ordering cancel_first=%v final=%s", cancelFirst, want)
    }
    for i := 0; i < 24; i++ {
        id, err := makeJob(fmt.Sprintf("probe-race-%d", i))
        if err != nil {
            return err
        }
        j, err := claim(ctx, db, 5*time.Second)
        if err != nil || j.ID != id {
            return fmt.Errorf("claim for race: %+v, %v", j, err)
        }
        type outcome struct {
            applied bool
            err error
        }
        completed := make(chan outcome, 1)
        go func() {
            applied, err := finish(ctx, db, j)
            completed <- outcome{applied, err}
        }()
        terminal, cancelErr := cancelJob(ctx, db, id)
        completion := <-completed
        if cancelErr != nil || completion.err != nil {
            return fmt.Errorf("race errors: cancel=%v finish=%v", cancelErr, completion.err)
        }
        want := "cancelled"
        if completion.applied {
            want = "succeeded"
        }
        if terminal.State != want {
            return fmt.Errorf("race produced state %s, expected %s", terminal.State, want)
        }
        if err := checkEffect(ctx, db, id, j.Attempt, completion.applied); err != nil {
            return err
        }
    }
    log.Print("PASS 24 concurrent cancel/complete attempts")
    return nil
}

func checkEffect(ctx context.Context, db *sql.DB, id, attempt int64, expected bool) error {
    var actual int64
    err := db.QueryRowContext(ctx, "SELECT attempt FROM job_effects WHERE job_id=$1", id).Scan(&actual)
    if expected && err == nil && actual == attempt || !expected && errors.Is(err, sql.ErrNoRows) {
        return nil
    }
    return fmt.Errorf("effect for job %d: expected=%v token=%d actual=%d err=%v", id, expected, attempt, actual, err)
}
