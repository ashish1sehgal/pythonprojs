import json
import math
import numpy as np
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from scipy import stats as scipy_stats
from google.cloud import storage

# --- ARCHITECTURAL CONSTANTS ---
PASS_1_STATS = {'Empty Count', 'Min', 'Max', 'Mean', 'Distinct Count'} 
PASS_2_STATS = {'Skewness', 'Kurtosis', 'Box Plot'}
MAX_SAMPLE_ROWS = 1000000 
MICRO_SAMPLE_ROWS = 2000

class EnterpriseProfilerEngine:
    def __init__(self, spark, pg_creds, source_platform, execution_tier):
        self.spark = spark
        self.pg_conn = psycopg2.connect(**pg_creds)
        self.source_platform = source_platform.upper() # e.g., 'BIGQUERY', 'ORACLE'
        self.execution_tier = execution_tier.upper()   # 'BASIC', 'ADVANCED', 'COMPLIANCE'
        self.dialect_cache = self._load_dialect_dictionary()

    def _load_dialect_dictionary(self):
        """Loads the stat2sql mapping. E.g., {'Mean': 'AVG(<<col>>)', 'Distinct Count': 'APPROX_COUNT_DISTINCT(<<col>>)'}"""
        cache = {}
        cur = self.pg_conn.cursor()
        cur.execute(f"SELECT stat, {self.source_platform.lower()} FROM stat2sql")
        for stat, template in cur.fetchall():
            if template: cache[stat.strip()] = template.strip()
        cur.close()
        return cache

    def _execute_native_pushdown(self, sql_query):
        """Forces the database engine to do the work. Spark only receives the aggregated results."""
        # In a real GCP environment, this uses the BigQuery spark connector with the 'query' option
        # or standard JDBC for Oracle/Postgres.
        return self.spark.read.format(self.source_platform.lower()) \
                   .option("query", sql_query) \
                   .load().collect()

    def _is_id_column(self, column_name):
        name_lower = column_name.lower()
        return name_lower.endswith(('_id', '_sk', '_key', 'uuid')) or name_lower in ['id', 'pk']

    # ==========================================
    # CORE EXECUTION ENGINE
    # ==========================================
    def execute_job(self, run_id, target_table, config_columns):
        
        # 0. Get Base Row Count for Circuit Breakers and Sampling percentages
        total_rows_result = self._execute_native_pushdown(f"SELECT COUNT(*) as ct FROM {target_table}")
        total_rows = total_rows_result[0]['ct']
        
        if total_rows == 0:
            return # Table is empty
            
        # Calculate native DB TABLESAMPLE percentage (e.g., 1,000,000 / 5,000,000,000 * 100 = 0.02%)
        sample_pct = min(100.0, (MAX_SAMPLE_ROWS / total_rows) * 100)
        sample_clause = f"TABLESAMPLE SYSTEM ({sample_pct} PERCENT)" if self.source_platform == 'BIGQUERY' else f"SAMPLE({sample_pct})"
        
        batches = [config_columns[i:i + 50] for i in range(0, len(config_columns), 50)]
        
        for batch_num, batch_cols in enumerate(batches):
            metrics_payloads = []
            hist_payloads = []
            freq_payloads = []
            pattern_payloads = []
            skipped_ops_master = {}
            
            # ==========================================
            # PASS 1: FULL POPULATION BASE SCAN
            # ==========================================
            pass_1_selects = []
            for col in batch_cols:
                c = col['column_name']
                for stat in col.get('stats', []):
                    if stat in PASS_1_STATS:
                        template = self.dialect_cache.get(stat)
                        if not template: continue
                        
                        sql_snippet = template.replace("<<col>>", f"{c}")
                        
                        # Exact Distinct Compliance Override
                        if stat == 'Distinct Count' and self.execution_tier == 'COMPLIANCE' and col.get('force_exact_distinct'):
                            sql_snippet = f"COUNT(DISTINCT {c})"
                            
                        pass_1_selects.append(f"{sql_snippet} AS `{c}__{stat}`")

            pass_1_sql = f"SELECT \n  " + ",\n  ".join(pass_1_selects) + f"\nFROM {target_table}"
            
            # Spark executes the massive SQL string natively on the database
            base_results = self._execute_native_pushdown(pass_1_sql)[0].asDict()

            # ==========================================
            # THE CIRCUIT BREAKER & PASS 2 SQL GEN
            # ==========================================
            pass_2_selects = []
            
            for col in batch_cols:
                c_name = col['column_name']
                skipped_ops_master[c_name] = {}
                is_id = self._is_id_column(c_name)
                
                # Retrieve Pass 1 Cardinality
                distinct_ct = base_results.get(f"{c_name}__Distinct Count", 0)
                uniqueness_ratio = (distinct_ct / total_rows) if total_rows > 0 else 0
                is_high_cardinality = uniqueness_ratio > 0.90
                
                for stat in col.get('stats', []):
                    if stat in PASS_2_STATS:
                        if is_id or is_high_cardinality:
                            skipped_ops_master[c_name][stat] = f"Skipped: High Cardinality ({uniqueness_ratio:.1%} unique) or PK."
                        else:
                            template = self.dialect_cache.get(stat)
                            if template:
                                # Uses EXACT percentile functions from stat2sql because we are running on a sample
                                sql_snippet = template.replace("<<col>>", f"{c_name}")
                                pass_2_selects.append(f"{sql_snippet} AS `{c_name}__{stat}`")

            # Execute Pass 2 (Shape) if any columns survived the circuit breaker
            shape_results = {}
            if pass_2_selects:
                pass_2_sql = f"SELECT \n  " + ",\n  ".join(pass_2_selects) + f"\nFROM {target_table} {sample_clause}"
                shape_results = self._execute_native_pushdown(pass_2_sql)[0].asDict()

            # ==========================================
            # PASS 3: SEMANTICS & HISTOGRAMS (NATIVE DB PUSHDOWN)
            # ==========================================
            for col in batch_cols:
                c_name = col['column_name']
                is_id = self._is_id_column(c_name)
                distinct_ct = base_results.get(f"{c_name}__Distinct Count", 0)
                
                # --- Categorical Top K (Native DB Pushdown) ---
                if col['data_type'] in ['STRING', 'VARCHAR'] and not is_id:
                    if distinct_ct > 1000:
                        skipped_ops_master[c_name]["Top-K Frequencies"] = f"Skipped: Distinct count ({distinct_ct}) > 1000 limit."
                    else:
                        top_k_sql = f"""
                            SELECT {c_name} AS val, COUNT(*) as ct 
                            FROM {target_table} {sample_clause}
                            WHERE {c_name} IS NOT NULL
                            GROUP BY {c_name} ORDER BY ct DESC LIMIT 10
                        """
                        for row in self._execute_native_pushdown(top_k_sql):
                            freq_payloads.append({"column_name": c_name, "value": str(row['val']), "count": row['ct']})
                
                # --- DB Native Histograms via WIDTH_BUCKET ---
                if col['data_type'] in ['INT', 'DECIMAL', 'FLOAT'] and col.get('viz_config', {}).get('histogram_bins'):
                    if is_high_cardinality and is_id:
                         skipped_ops_master[c_name]["Histograms"] = "Skipped: ID Columns do not require histograms."
                    else:
                        exact_min = base_results.get(f"{c_name}__Min")
                        exact_max = base_results.get(f"{c_name}__Max")
                        num_bins = col['viz_config']['histogram_bins']
                        
                        if exact_min is not None and exact_max is not None and exact_min != exact_max:
                            hist_sql = f"""
                                SELECT WIDTH_BUCKET({c_name}, {exact_min}, {exact_max}, {num_bins}) AS bucket_id, COUNT(*) AS freq 
                                FROM {target_table} {sample_clause}
                                WHERE {c_name} IS NOT NULL
                                GROUP BY bucket_id ORDER BY bucket_id
                            """
                            # Extrapolate Sample Frequencies back to Population scale
                            extrapolation_multiplier = total_rows / MAX_SAMPLE_ROWS if total_rows > MAX_SAMPLE_ROWS else 1
                            
                            for row in self._execute_native_pushdown(hist_sql):
                                hist_payloads.append({
                                    "column_name": c_name, 
                                    "bucket": row['bucket_id'], 
                                    "count": int(row['freq'] * extrapolation_multiplier)
                                })

            # ==========================================
            # PASS 4: SCIPY DRIVER MATH (THE ONLY DATA EXTRACT)
            # ==========================================
            # ... (SciPy Logic remains unchanged, explicitly fetching LIMIT 2000 for pure Python math) ...

            # ==========================================
            # UNPIVOT & INBOX DROP
            # ==========================================
            for col in batch_cols:
                c_name = col['column_name']
                metrics_payloads.append({
                    "run_id": run_id,
                    "column_name": c_name,
                    "mean_val": base_results.get(f"{c_name}__Mean"),
                    "distinct_count": base_results.get(f"{c_name}__Distinct Count"),
                    "skewness_val": shape_results.get(f"{c_name}__Skewness"), 
                    "skipped_operations": json.dumps(skipped_ops_master[c_name]) 
                })

            self._drop_to_data_inbox(run_id, batch_num, metrics_payloads, hist_payloads, freq_payloads, pattern_payloads)

    def _drop_to_data_inbox(self, run_id, batch_num, metrics, hists, freqs, patterns):
        # Implementation to write to GCS
        pass
