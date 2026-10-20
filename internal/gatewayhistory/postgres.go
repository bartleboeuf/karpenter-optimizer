package gatewayhistory

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is a durable Store backed by a Postgres (e.g. CNPG) instance.
// It survives process restarts/redeploys, so cost history accumulates across
// releases. Enabled when GATEWAY_HISTORY_DSN is set.
type PostgresStore struct {
	pool      *pgxpool.Pool
	retention time.Duration
	now       func() time.Time
}

const dailySchema = `
CREATE TABLE IF NOT EXISTS gateway_history_daily (
    dim   TEXT NOT NULL,
    value TEXT NOT NULL DEFAULT '',
    day   DATE NOT NULL,
    spend DOUBLE PRECISION NOT NULL DEFAULT 0,
    tokens BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (dim, value, day)
);
CREATE INDEX IF NOT EXISTS idx_gwh_dim_day ON gateway_history_daily (dim, day);
`

// dayDelta is one folded (dim, value, day) increment ready to upsert.
type dayDelta struct {
	dim    string
	value  string
	day    string
	spend  float64
	tokens int64
}

// NewPostgres connects to the DSN, verifies it, and ensures the schema exists.
// It returns an error if the database is unreachable so the caller can fall
// back to the in-memory store.
func NewPostgres(ctx context.Context, dsn string, retention time.Duration) (*PostgresStore, error) {
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("gatewayhistory: new pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("gatewayhistory: ping: %w", err)
	}
	schemaCtx, sCancel := context.WithTimeout(ctx, 10*time.Second)
	defer sCancel()
	if _, err := pool.Exec(schemaCtx, dailySchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("gatewayhistory: schema: %w", err)
	}
	return &PostgresStore{pool: pool, retention: retention, now: time.Now}, nil
}

// Add folds each delta into its UTC day's running total (incrementing any
// existing row). Best-effort retention purge runs alongside.
func (p *PostgresStore) Add(deltas []Delta) error {
	if len(deltas) == 0 {
		return nil
	}
	// Fold deltas by (dim, value, day) so one upsert per series per day.
	folded := map[string]*dayDelta{}
	var order []string
	for _, d := range deltas {
		if !validDim(d.Dim) {
			continue
		}
		day := dayKey(d.TS)
		key := accKey(d.Dim, d.Value, day)
		if e, ok := folded[key]; ok {
			e.spend += d.Spend
			e.tokens += d.Tokens
			continue
		}
		folded[key] = &dayDelta{dim: d.Dim, value: d.Value, day: day, spend: d.Spend, tokens: d.Tokens}
		order = append(order, key)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, key := range order {
		d := folded[key]
		_, err := tx.Exec(ctx, `
            INSERT INTO gateway_history_daily (dim, value, day, spend, tokens)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (dim, value, day)
            DO UPDATE SET spend = gateway_history_daily.spend + excluded.spend,
                          tokens = gateway_history_daily.tokens + excluded.tokens`,
			d.dim, d.value, d.day, d.spend, d.tokens)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// Best-effort retention purge (once per Add; cheap indexed delete).
	cutoff := dayKey(p.now().Add(-p.retention))
	_, _ = p.pool.Exec(ctx, `DELETE FROM gateway_history_daily WHERE day < $1`, cutoff)
	return nil
}

// Daily returns per-day, per-value totals for dim, oldest first, for days at
// or after since (UTC).
func (p *PostgresStore) Daily(dim string, since time.Time) ([]DayRow, error) {
	if !validDim(dim) {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := p.pool.Query(ctx, `
        SELECT day, value, spend, tokens
        FROM gateway_history_daily
        WHERE dim = $1 AND day >= $2
        ORDER BY day, value`, dim, since.UTC().Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DayRow
	for rows.Next() {
		var day time.Time
		var value string
		var spend float64
		var tokens int64
		if err := rows.Scan(&day, &value, &spend, &tokens); err != nil {
			return nil, err
		}
		d := day.UTC().Format("2006-01-02")
		out = append(out, DayRow{Day: d, DayTS: d + "T00:00:00Z", Value: value, Spend: spend, Tokens: tokens})
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
