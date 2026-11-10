package main

import (
    "context"
    "database/sql"
    "time"
)

type payload struct {
    DurationMS int  `json:"duration_ms"`
    Fail       bool `json:"fail"`
}

type job struct {
    ID      int64   `json:"id"`
    State   string  `json:"state"`
    Attempt int64   `json:"attempt"`
    Payload payload `json:"payload"`
}

const jobColumns = "id,state,attempt,duration_ms,should_fail"

type scanner interface{ Scan(...any) error }

func readJob(row scanner) (j job, err error) {
    err = row.Scan(&j.ID, &j.State, &j.Attempt, &j.Payload.DurationMS, &j.Payload.Fail)
    return
}

func claim(ctx context.Context, db *sql.DB, lease time.Duration) (job, error) {
    return readJob(db.QueryRowContext(ctx, `
        WITH candidate AS (
            SELECT id FROM jobs
            WHERE state = 'queued'
               OR (state = 'running' AND lease_until <= statement_timestamp())
            ORDER BY id
            FOR UPDATE SKIP LOCKED LIMIT 1
        )
        UPDATE jobs AS j
        SET state = 'running', attempt = j.attempt + 1,
            lease_until = clock_timestamp() + $1::bigint * interval '1 millisecond'
        FROM candidate AS c WHERE j.id = c.id
        RETURNING j.id,j.state,j.attempt,j.duration_ms,j.should_fail`, lease.Milliseconds()))
}

func finish(ctx context.Context, db *sql.DB, j job) (bool, error) {
    tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
    if err != nil {
        return false, err
    }
    defer tx.Rollback()
    // Acquire the lock before checking wall time, since waiting may cross expiry.
    var id int64
    err = tx.QueryRowContext(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID).Scan(&id)
    if err != nil {
        return false, err
    }
    state := "succeeded"
    if j.Payload.Fail {
        state = "failed"
    }
    result, err := tx.ExecContext(ctx, `
        UPDATE jobs SET state=$3, lease_until=NULL
        WHERE id=$1 AND attempt=$2 AND state='running' AND lease_until>clock_timestamp()`,
        j.ID, j.Attempt, state)
    if err != nil {
        return false, err
    }
    n, err := result.RowsAffected()
    if err != nil || n == 0 {
        return false, err
    }
    if state == "succeeded" {
        _, err = tx.ExecContext(ctx, "INSERT INTO job_effects(job_id,attempt) VALUES ($1,$2)", j.ID, j.Attempt)
        if err != nil {
            return false, err
        }
    }
    if err := tx.Commit(); err != nil {
        return false, err
    }
    return true, nil
}

func cancelJob(ctx context.Context, db *sql.DB, id int64) (job, error) {
    j, err := readJob(db.QueryRowContext(ctx, `
        UPDATE jobs SET state='cancelled', lease_until=NULL
        WHERE id=$1 AND state IN ('queued','running') RETURNING `+jobColumns, id))
    if err != sql.ErrNoRows {
        return j, err
    }
    return readJob(db.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM jobs WHERE id=$1", id))
}
