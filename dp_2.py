# --- Assuming Pass 1 is complete and we are ready for Pass 3 ---

# 1. Initialize arrays to track what we are asking the DB to do
freq_cols = []
pat_cols = []
hist_cols = []

select_expressions = []
grouping_sets = []
aliases = []

for col in batch_cols:
    c_name = col['column_name']
    
    # Check circuit breakers from Pass 1 (Pseudo-code for brevity)
    is_safe_freq = ... 
    is_safe_pat = ...
    is_safe_hist = ... 
    
    # A. Add Frequency Logic
    if is_safe_freq:
        freq_cols.append(c_name)
        alias = f"{c_name}_raw"
        select_expressions.append(f"{c_name} AS {alias}")
        grouping_sets.append(f"({alias})")
        aliases.append(alias)
        
    # B. Add Pattern Logic
    if is_safe_pat:
        pat_cols.append(c_name)
        alias = f"{c_name}_pat"
        pat_expr = f"REGEXP_REPLACE(REGEXP_REPLACE({c_name}, '[A-Za-z]', 'A'), '[0-9]', 'N')"
        select_expressions.append(f"{pat_expr} AS {alias}")
        grouping_sets.append(f"({alias})")
        aliases.append(alias)
        
    # C. Add Histogram Logic
    if is_safe_hist:
        exact_min = base_results.get(f"{c_name}__Min")
        exact_max = base_results.get(f"{c_name}__Max")
        num_bins = col.get('viz_config', {}).get('histogram_bins', 20)
        
        # Prevent division by zero if Min == Max
        if exact_min is not None and exact_max is not None and exact_min != exact_max:
            hist_cols.append(c_name)
            alias = f"{c_name}_hist"
            hist_expr = f"WIDTH_BUCKET({c_name}, {exact_min}, {exact_max}, {num_bins})"
            select_expressions.append(f"{hist_expr} AS {alias}")
            grouping_sets.append(f"({alias})")
            aliases.append(alias)

# 2. Build the Mega-Query
if grouping_sets:
    mega_sql = f"""
        WITH SampledData AS (
            SELECT {', '.join(select_expressions)} 
            FROM {target_table} {sample_clause}
        )
        SELECT {', '.join(aliases)}, COUNT(*) as ct
        FROM SampledData
        GROUP BY GROUPING SETS ({', '.join(grouping_sets)})
    """
    
    # 3. Execute Natively (One Single Network Hop!)
    mega_results = self._execute_native_pushdown(mega_sql)
    
    # 4. Parse the Sparse Matrix efficiently
    freq_dict = {c: [] for c in freq_cols}
    pat_dict = {c: [] for c in pat_cols}
    
    # Extrapolation for Histograms
    extrapolation_multiplier = total_rows / MAX_SAMPLE_ROWS if total_rows > MAX_SAMPLE_ROWS else 1
    
    for row in mega_results:
        # Check Frequencies
        for c in freq_cols:
            alias = f"{c}_raw"
            if row[alias] is not None:
                freq_dict[c].append((row[alias], row['ct']))
                
        # Check Patterns
        for c in pat_cols:
            alias = f"{c}_pat"
            if row[alias] is not None:
                pat_dict[c].append((row[alias], row['ct']))
                
        # Check Histograms
        for c in hist_cols:
            alias = f"{c}_hist"
            if row[alias] is not None:
                hist_payloads.append({
                    "column_name": c, 
                    "bucket": row[alias], 
                    "count": int(row['ct'] * extrapolation_multiplier)
                })

    # 5. Sort and limit Frequencies and Patterns to Top 10 in Python
    for c, vals in freq_dict.items():
        vals.sort(key=lambda x: x[1], reverse=True)
        for val, ct in vals[:10]:
            freq_payloads.append({"column_name": c, "value": str(val), "count": ct})
            
    for c, vals in pat_dict.items():
        vals.sort(key=lambda x: x[1], reverse=True)
        for val, ct in vals[:10]:
            pattern_payloads.append({"column_name": c, "pattern": str(val), "count": ct})