package servingdurable

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is a durable Store backed by a Postgres (e.g. CNPG) instance.
// It survives process restarts/redeploys, so serving history accumulates
// across releases. Enabled when a DSN is configured (SERVING_HISTORY_DSN or
// the shared GATEWAY_HISTORY_DB_* parts, reusing one CNPG instance).
type PostgresStore struct {
	pool      *pgxpool.Pool
	retention time.Duration
	now       func() time.Time
}

const schema = `
CREATE TABLE IF NOT EXISTS serving_history (
    pod_key TEXT NOT NULL,
    t TIMESTAMPTZ NOT NULL,
    online BOOLEAN NOT NULL,
    kv_cache_pct DOUBLE PRECISION,
    running INTEGER NOT NULL DEFAULT 0,
    waiting INTEGER NOT NULL DEFAULT 0,
    preemptions BIGINT NOT NULL DEFAULT 0,
    ttft_p95 DOUBLE PRECISION,
    e2e_p95 DOUBLE PRECISION,
    tokens_per_sec DOUBLE PRECISION,
    requests_per_sec DOUBLE PRECISION,
    PRIMARY KEY (pod_key, t)
);
CREATE INDEX IF NOT EXISTS idx_serving_history_t ON serving_history (t);
`

// NewPostgres connects to the DSN, verifies it, and ensures the schema exists.
// It returns an error if the database is unreachable so the caller can skip
// durable storage (the in-memory store still serves the short windows).
func NewPostgres(ctx context.Context, dsn string, retention time.Duration) (*PostgresStore, error) {
	if retention <= 0 {
		retention = 7 * 24 * time.Hour
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("servingdurable: new pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("servingdurable: ping: %w", err)
	}
	schemaCtx, sCancel := context.WithTimeout(ctx, 10*time.Second)
	defer sCancel()
	if _, err := pool.Exec(schemaCtx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("servingdurable: schema: %w", err)
	}
	return &PostgresStore{pool: pool, retention: retention, now: time.Now}, nil
}

// Add upserts each raw sample on (pod_key, t). Best-effort retention purge
// runs alongside.
func (p *PostgresStore) Add(points []Point) error {
	if len(points) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, pt := range points {
		_, err := tx.Exec(ctx, `
            INSERT INTO serving_history (pod_key, t, online, kv_cache_pct, running, waiting, preemptions, ttft_p95, e2e_p95, tokens_per_sec, requests_per_sec)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (pod_key, t) DO NOTHING`,
			pt.PodKey, pt.T, pt.Online, pt.KVCachePercent, pt.Running, pt.Waiting, pt.Preemptions,
			pt.TTFTP95Seconds, pt.E2EP95Seconds, pt.TokensPerSec, pt.RequestsPerSec)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Best-effort retention purge (indexed delete).
	cutoff := p.now().Add(-p.retention)
	_, _ = p.pool.Exec(ctx, `DELETE FROM serving_history WHERE t < $1`, cutoff)
	return nil
}

// Query returns all pods' samples at or after since, grouped by pod key, each
// slice oldest first.
func (p *PostgresStore) Query(since time.Time) (map[string][]Point, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := p.pool.Query(ctx, `
        SELECT pod_key, t, online, kv_cache_pct, running, waiting, preemptions, ttft_p95, e2e_p95, tokens_per_sec, requests_per_sec
        FROM serving_history
        WHERE t >= $1
        ORDER BY pod_key, t`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]Point)
	for rows.Next() {
		var pt Point
		var t time.Time
		if err := rows.Scan(&pt.PodKey, &t, &pt.Online, &pt.KVCachePercent, &pt.Running, &pt.Waiting,
			&pt.Preemptions, &pt.TTFTP95Seconds, &pt.E2EP95Seconds, &pt.TokensPerSec, &pt.RequestsPerSec); err != nil {
			return nil, err
		}
		pt.T = t
		out[pt.PodKey] = append(out[pt.PodKey], pt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (p *PostgresStore) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}
