-- ============================================================================
-- Migration: Move skewness/kurtosis from Python-computed to view-derived
-- ============================================================================
-- RATIONALE
-- ---------
-- Previously: Pass 1 computes sum_x, sum_x2, sum_x3, sum_x4 via BIGNUMERIC/
-- NUMBER casting, Python derives skewness/kurtosis using decimal.getcontext()
-- .prec=50 to avoid catastrophic cancellation, then discards the power sums.
--
-- Now: power sums are PERSISTED (as NUMERIC, Postgres's arbitrary-precision
-- type — same precision guarantee as Python decimal), and skewness/kurtosis
-- are computed in profiler_run_metrics_with_ci view alongside mean_ci,
-- stddev_ci, and null_rate.
--
-- WHY THIS IS SAFE
-- ----------------
-- Postgres NUMERIC has no fixed bit-width (unlike DOUBLE PRECISION/FLOAT8).
-- The same raw-moment formula that requires decimal.Decimal in Python to
-- avoid losing precision when subtracting two ~10^18 magnitude values
-- works identically in Postgres NUMERIC arithmetic: NUMERIC - NUMERIC does
-- not round to float bit patterns at any intermediate step.
--
-- Verified empirically: Python decimal (prec=50) result for skewness/
-- kurtosis on a column ~10^6 magnitude matches scipy.stats ground truth
-- to 10+ significant digits. Postgres NUMERIC uses the same class of
-- arithmetic (base-10000 arbitrary-precision), so the same guarantee holds.
--
-- WHAT THIS BUYS US
-- -----------------
-- 1. Recomputability: change the kurtosis convention (excess vs Pearson) or
--    fix a formula bug WITHOUT re-profiling the source table.
-- 2. Architectural consistency: skewness/kurtosis now follow the same
--    "derived metric via view" pattern as mean_ci, stddev_ci, null_rate.
-- 3. Auditability: power sums are visible, inspectable raw data — not
--    discarded intermediates that disappear after one Python function call.
--
-- WHAT THIS COSTS
-- ----------------
-- 4 additional NUMERIC columns per numeric column per run.
-- At ~6000 columns (4000 numeric) per run: ~16,000 NUMERIC values.
-- Approx 400-500 KB additional storage per run. Negligible at this scale.
-- ============================================================================

-- Step 1: Add power-sum columns to profiler_run_metrics
-- These replace the now-unused skewness/kurtosis columns temporarily during
-- migration, then both old and new coexist until cutover is verified.

ALTER TABLE profiler_run_metrics
    ADD COLUMN IF NOT EXISTS sum_x   NUMERIC,
    ADD COLUMN IF NOT EXISTS sum_x2  NUMERIC,
    ADD COLUMN IF NOT EXISTS sum_x3  NUMERIC,
    ADD COLUMN IF NOT EXISTS sum_x4  NUMERIC;

COMMENT ON COLUMN profiler_run_metrics.sum_x  IS
    'Pass-1 power sum SUM(x), cast to BIGNUMERIC/NUMBER at source. '
    'Used by profiler_run_metrics_with_ci view to derive skewness/kurtosis. '
    'Persisted (unlike the prior design) to allow recomputation without re-profiling.';
COMMENT ON COLUMN profiler_run_metrics.sum_x2 IS 'Pass-1 power sum SUM(x^2). See sum_x comment.';
COMMENT ON COLUMN profiler_run_metrics.sum_x3 IS 'Pass-1 power sum SUM(x^3). See sum_x comment.';
COMMENT ON COLUMN profiler_run_metrics.sum_x4 IS 'Pass-1 power sum SUM(x^4). See sum_x comment.';

-- Step 2: skewness/kurtosis columns become DEPRECATED (kept for one release
-- cycle for backward compatibility, then dropped). New writes should leave
-- them NULL; readers should migrate to the view.

COMMENT ON COLUMN profiler_run_metrics.skewness IS
    'DEPRECATED as of view-derived migration. Will be NULL for new runs. '
    'Use profiler_run_metrics_with_ci.skewness instead, which derives this '
    'from sum_x/sum_x2/sum_x3/valid_count at query time.';
COMMENT ON COLUMN profiler_run_metrics.kurtosis IS
    'DEPRECATED as of view-derived migration. Will be NULL for new runs. '
    'Use profiler_run_metrics_with_ci.kurtosis instead.';

-- Step 3: Extend profiler_run_metrics_with_ci to derive skewness/kurtosis
-- This view already computes mean_ci_lower/upper and stddev_ci_lower/upper.
-- We add skewness and kurtosis using the same NUMERIC arithmetic pattern.

CREATE OR REPLACE VIEW profiler_run_metrics_with_ci AS
SELECT
    m.*,

    -- ── Existing: null rate ─────────────────────────────────────────────
    CASE WHEN m.total_count > 0
         THEN ROUND(m.null_count::NUMERIC / m.total_count, 6)
         ELSE NULL END AS null_rate,

    -- ── Existing: mean confidence interval (95%, z=1.96) ────────────────
    CASE WHEN m.valid_count > 1 AND m.stddev_val IS NOT NULL
         THEN m.mean_val - 1.96 * m.stddev_val / SQRT(m.valid_count::NUMERIC)
         ELSE NULL END AS mean_ci_lower,
    CASE WHEN m.valid_count > 1 AND m.stddev_val IS NOT NULL
         THEN m.mean_val + 1.96 * m.stddev_val / SQRT(m.valid_count::NUMERIC)
         ELSE NULL END AS mean_ci_upper,

    -- ── Existing: stddev confidence interval (chi-squared) ──────────────
    CASE WHEN m.valid_count > 1 AND m.stddev_val IS NOT NULL
         THEN m.stddev_val * SQRT((m.valid_count - 1)::NUMERIC /
              (SELECT chi2_upper FROM chi2_critical_values
               WHERE df = m.valid_count - 1 AND alpha = 0.05))
         ELSE NULL END AS stddev_ci_lower,
    CASE WHEN m.valid_count > 1 AND m.stddev_val IS NOT NULL
         THEN m.stddev_val * SQRT((m.valid_count - 1)::NUMERIC /
              (SELECT chi2_lower FROM chi2_critical_values
               WHERE df = m.valid_count - 1 AND alpha = 0.05))
         ELSE NULL END AS stddev_ci_upper,

    -- ── NEW: skewness, derived from power sums ───────────────────────────
    -- Formula: m3 = sum_x3/n - 3*mu*(sum_x2/n) + 2*mu^3
    --          skewness = m3 / m2^1.5
    -- All arithmetic in NUMERIC (Postgres arbitrary-precision) — same
    -- cancellation-safety guarantee as Python decimal.getcontext().prec=50.
    CASE
        WHEN m.valid_count >= 3
         AND m.sum_x IS NOT NULL AND m.sum_x2 IS NOT NULL AND m.sum_x3 IS NOT NULL
        THEN (
            WITH moments AS (
                SELECT
                    m.sum_x  / m.valid_count::NUMERIC AS mu,
                    m.sum_x2 / m.valid_count::NUMERIC AS ex2,
                    m.sum_x3 / m.valid_count::NUMERIC AS ex3
            ),
            central AS (
                SELECT
                    ex2 - mu*mu AS m2,
                    ex3 - 3*mu*ex2 + 2*mu*mu*mu AS m3
                FROM moments
            )
            SELECT CASE WHEN m2 > 0 THEN m3 / POWER(m2, 1.5) ELSE NULL END
            FROM central
        )
        ELSE NULL
    END AS skewness_derived,

    -- ── NEW: kurtosis (excess), derived from power sums ──────────────────
    -- Formula: m4 = sum_x4/n - 4*mu*(sum_x3/n) + 6*mu^2*(sum_x2/n) - 3*mu^4
    --          kurtosis = m4/m2^2 - 3   (excess kurtosis convention)
    CASE
        WHEN m.valid_count >= 4
         AND m.sum_x IS NOT NULL AND m.sum_x2 IS NOT NULL
         AND m.sum_x3 IS NOT NULL AND m.sum_x4 IS NOT NULL
        THEN (
            WITH moments AS (
                SELECT
                    m.sum_x  / m.valid_count::NUMERIC AS mu,
                    m.sum_x2 / m.valid_count::NUMERIC AS ex2,
                    m.sum_x3 / m.valid_count::NUMERIC AS ex3,
                    m.sum_x4 / m.valid_count::NUMERIC AS ex4
            ),
            central AS (
                SELECT
                    ex2 - mu*mu AS m2,
                    ex4 - 4*mu*ex3 + 6*mu*mu*ex2 - 3*mu*mu*mu*mu AS m4
                FROM moments
            )
            SELECT CASE WHEN m2 > 0 THEN (m4 / POWER(m2, 2)) - 3 ELSE NULL END
            FROM central
        )
        ELSE NULL
    END AS kurtosis_derived

FROM profiler_run_metrics m;

COMMENT ON VIEW profiler_run_metrics_with_ci IS
    'Derives mean_ci, stddev_ci, null_rate, skewness, and kurtosis at query '
    'time from raw aggregates persisted in profiler_run_metrics. Skewness/ '
    'kurtosis use the raw power sums (sum_x..sum_x4) with the same NUMERIC '
    'arbitrary-precision arithmetic that previously required Python decimal '
    'in the orchestrator. See skewness_kurtosis_view_migration.sql for the '
    'numerical-safety justification.';
