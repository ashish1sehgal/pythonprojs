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
        self.source_platform = source_platform.upper()
        self.execution_tier = execution_tier.upper()   
        self.dialect_cache = self._load_dialect_dictionary()

    def _load_dialect_dictionary(self):
        cache = {}
        cur = self.pg_conn.cursor()
        cur.execute(f"SELECT stat, {self.source_platform.lower()} FROM stat2sql")
        for stat, template in cur.fetchall():
            if template: cache[stat.strip()] = template.strip()
        cur.close()
        return cache

    def _execute_native_pushdown(self, sql_query):
        """Forces the source DB to execute the query. Spark only orchestrates."""
        return self.spark.read.format(self.source_platform.lower()) \
                   .option("query", sql_query) \
                   .load().collect()

    def _is_id_column(self, column_name):
        name_lower = column_name.lower()
        return name_lower.endswith(('_id', '_sk', '_key', 'uuid')) or name_lower in ['id', 'pk']

    def _get_pattern_expr(self, col_name):
        """Standard SQL Regex to replace letters with A and numbers with N."""
        return f"REGEXP_REPLACE(REGEXP_REPLACE({col_name}, '[A-Za-z]', 'A'), '[0-9]', 'N')"

    def _evaluate_normality(self, data_series, test_name='Shapiro-Wilk'):
        """Executes purely in Python using the Micro-Sample."""
        clean_data = data_series.dropna().astype(float)
        if len(clean_data) < 4 or clean_data.nunique() <= 1:
            return (test_name, None, None, None)
        try:
            stat, p_value = scipy_stats.shapiro(clean_data)
            return (test_name, float(stat), float(p_value), bool(p_value > 0.05))
        except Exception:
            return (test_name, None, None, None)

    # ==========================================
    # CORE EXECUTION ENGINE
    # ==========================================
    def execute_job(self, run_id, target_table, config_columns):
        
        total_rows_result = self._execute_native_pushdown(f"SELECT COUNT(*) as ct FROM {target_table}")
        total_rows = total_rows_result[0]['ct']
        if total_rows == 0: return 
            
        sample_pct = min(100.0, (MAX_SAMPLE_ROWS / total_rows) * 100)
        sample_clause = f"TABLESAMPLE SYSTEM ({sample_pct} PERCENT)" if self.source_platform == 'BIGQUERY' else f"SAMPLE({sample_pct})"
        
        batches = [config_columns[i:i + 50] for i in range(0, len(config_columns), 50)]
        
        for batch_num, batch_cols in enumerate(batches):
            metrics_payloads, hist_payloads = [], []
            freq_payloads, pattern_payloads = [], []
            skipped_ops_master = {c['column_name']: {} for c in batch_cols}
            
            # ==========================================
            # PASS 1: FULL POPULATION BASE SCAN
            # ==========================================
            pass_1_selects = []
            for col in batch_cols:
                c = col['column_name']
                is_string = col['data_type'] in ['STRING', 'VARCHAR']
                
                for stat in col.get('stats', []):
                    if stat in PASS_1_STATS:
                        template = self.dialect_cache.get(stat)
                        if not template: continue
                        sql_snippet = template.replace("<<col>>", f"{c}")
                        
                        # Compliance Override
                        if stat == 'Distinct Count' and self.execution_tier == 'COMPLIANCE' and col.get('force_exact_distinct'):
                            sql_snippet = f"COUNT(DISTINCT {c})"
                            
                        pass_1_selects.append(f"{sql_snippet} AS `{c}__{stat}`")
                
                # NATIVE PATTERN CARDINALITY: Inject Approx Distinct for string patterns
                if is_string and not self._is_id_column(c):
                    pat_expr = self._get_pattern_expr(c)
                    pass_1_selects.append(f"APPROX_COUNT_DISTINCT({pat_expr}) AS `{c}__Pattern_Distinct_Count`")

            pass_1_sql = f"SELECT \n  " + ",\n  ".join(pass_1_selects) + f"\nFROM {target_table}"
            base_results = self._execute_native_pushdown(pass_1_sql)[0].asDict()

            # ==========================================
            # PASS 2 & 3: SHAPE, GROUPS & HISTOGRAMS (SAMPLED)
            # ==========================================
            pass_2_selects = []
            freq_cols_to_run = []
            pat_cols_to_run = []
            
            for col in batch_cols:
                c_name = col['column_name']
                is_id = self._is_id_column(c_name)
                
                distinct_ct = base_results.get(f"{c_name}__Distinct Count", 0)
                uniqueness_ratio = (distinct_ct / total_rows) if total_rows > 0 else 0
                is_high_cardinality = uniqueness_ratio > 0.90
                
                # 1. Distribute Shape Queries (Skewness, Kurtosis, Medians)
                for stat in col.get('stats', []):
                    if stat in PASS_2_STATS:
                        if is_id or is_high_cardinality:
                            skipped_ops_master[c_name][stat] = "Skipped: High Cardinality or PK."
                        else:
                            template = self.dialect_cache.get(stat)
                            if template:
                                pass_2_selects.append(f"{template.replace('<<col>>', c_name)} AS `{c_name}__{stat}`")

                # 2. Gate Frequency Columns
                if col['data_type'] in ['STRING', 'VARCHAR'] and not is_id:
                    if distinct_ct > 1000:
                        skipped_ops_master[c_name]["Top-K"] = f"Skipped: Cardinality ({distinct_ct}) > 1000."
                    else:
                        freq_cols_to_run.append(c_name)
                        
                # 3. Gate Pattern Columns
                if col['data_type'] in ['STRING', 'VARCHAR'] and not is_id:
                    pat_dist_ct = base_results.get(f"{c_name}__Pattern_Distinct_Count", 0)
                    if pat_dist_ct > 100:
                        skipped_ops_master[c_name]["Patterns"] = f"Skipped: Distinct patterns ({pat_dist_ct}) > 100."
                    else:
                        pat_cols_to_run.append(c_name)

            # --- EXECUTE PASS 2 (SHAPE) ---
            shape_results = {}
            if pass_2_selects:
                pass_2_sql = f"SELECT \n  " + ",\n  ".join(pass_2_selects) + f"\nFROM {target_table} {sample_clause}"
                shape_results = self._execute_native_pushdown(pass_2_sql)[0].asDict()

            # --- EXECUTE PASS 3A: FREQUENCIES (GROUPING SETS) ---
            if freq_cols_to_run:
                g_sets = ", ".join([f"({c})" for c in freq_cols_to_run])
                s_cols = ", ".join(freq_cols_to_run)
                freq_sql = f"""
                    SELECT {s_cols}, COUNT(*) as ct 
                    FROM {target_table} {sample_clause} 
                    GROUP BY GROUPING SETS ({g_sets})
                """
                freq_res = self._execute_native_pushdown(freq_sql)
                
                # Parse Grouping Sets Output
                freq_dict = {c: [] for c in freq_cols_to_run}
                for row in freq_res:
                    for c in freq_cols_to_run:
                        if row[c] is not None:
                            freq_dict[c].append((row[c], row['ct']))
                
                for c, vals in freq_dict.items():
                    vals.sort(key=lambda x: x[1], reverse=True)
                    for val, ct in vals[:10]: # Top 10 Only
                        freq_payloads.append({"column_name": c, "value": str(val), "count": ct})

            # --- EXECUTE PASS 3B: PATTERNS (GROUPING SETS) ---
            if pat_cols_to_run:
                pat_selects = [f"{self._get_pattern_expr(c)} AS {c}_pat" for c in pat_cols_to_run]
                pat_aliases = [f"{c}_pat" for c in pat_cols_to_run]
                g_sets = ", ".join([f"({a})" for a in pat_aliases])
                
                pat_sql = f"""
                    WITH PatternBase AS (
                        SELECT {', '.join(pat_selects)} FROM {target_table} {sample_clause}
                    )
                    SELECT {', '.join(pat_aliases)}, COUNT(*) as ct
                    FROM PatternBase GROUP BY GROUPING SETS ({g_sets})
                """
                pat_res = self._execute_native_pushdown(pat_sql)
                
                pat_dict = {c: [] for c in pat_cols_to_run}
                for row in pat_res:
                    for c, alias in zip(pat_cols_to_run, pat_aliases):
                        if row[alias] is not None:
                            pat_dict[c].append((row[alias], row['ct']))
                            
                for c, vals in pat_dict.items():
                    vals.sort(key=lambda x: x[1], reverse=True)
                    for val, ct in vals[:10]: 
                        pattern_payloads.append({"column_name": c, "pattern": str(val), "count": ct})

            # ==========================================
            # PASS 4: SCIPY NORMALITY (MICRO-SAMPLE EXTRACT)
            # ==========================================
            normality_cols = [c['column_name'] for c in batch_cols if c.get('normality_test') and not self._is_id_column(c['column_name'])]
            normality_results = {}
            
            if normality_cols:
                # Extract exactly 2000 rows into Python Memory
                norm_sql = f"SELECT {', '.join(normality_cols)} FROM {target_table} LIMIT {MICRO_SAMPLE_ROWS}"
                micro_sample_data = self._execute_native_pushdown(norm_sql)
                df_micro = pd.DataFrame([row.asDict() for row in micro_sample_data])
                
                for c_name in normality_cols:
                    t_name, t_stat, p_val, is_norm = self._evaluate_normality(df_micro[c_name], 'Shapiro-Wilk')
                    normality_results[c_name] = {"test": t_name, "is_normal": is_norm, "p_value": p_val}

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
                    "normality_test": normality_results.get(c_name, {}).get("test"),
                    "is_normal": normality_results.get(c_name, {}).get("is_normal"),
                    "skipped_operations": json.dumps(skipped_ops_master[c_name]) 
                })

            self._drop_to_data_inbox(run_id, batch_num, metrics_payloads, hist_payloads, freq_payloads, pattern_payloads)

    def _drop_to_data_inbox(self, run_id, batch_num, metrics, hists, freqs, patterns):
        # Implementation to write to GCS as JSON
        pass
