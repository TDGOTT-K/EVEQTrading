CREATE TABLE IF NOT EXISTS item_catalog (
    type_id INTEGER PRIMARY KEY,
    type_name TEXT NOT NULL,
    type_name_en TEXT,
    type_name_zh TEXT,
    group_id INTEGER,
    category_id INTEGER,
    market_group_id INTEGER,
    volume REAL,
    packaged_volume REAL,
    meta_level INTEGER,
    market_group TEXT,
    is_published INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_group_catalog (
    market_group_id INTEGER PRIMARY KEY,
    parent_group_id INTEGER,
    market_group_name TEXT NOT NULL,
    market_group_name_en TEXT,
    market_group_name_zh TEXT,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reference_data_import_run (
    import_run_id TEXT PRIMARY KEY,
    import_kind TEXT NOT NULL,
    source_path TEXT NOT NULL,
    source_build INTEGER,
    source_release_date TEXT,
    imported_at TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_location_catalog (
    location_id INTEGER PRIMARY KEY,
    location_name TEXT NOT NULL,
    location_kind TEXT NOT NULL,
    region_id INTEGER,
    solar_system_id INTEGER,
    is_npc INTEGER NOT NULL DEFAULT 0,
    is_trade_hub_candidate INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_history_daily (
    type_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    market_date TEXT NOT NULL,
    average_price REAL NOT NULL,
    highest_price REAL NOT NULL,
    lowest_price REAL NOT NULL,
    volume REAL NOT NULL,
    order_count INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,
    source TEXT NOT NULL,
    coverage_status TEXT NOT NULL,
    PRIMARY KEY (type_id, region_id, market_date)
);

CREATE TABLE IF NOT EXISTS market_order_snapshot_head (
    snapshot_id TEXT PRIMARY KEY,
    captured_at TEXT NOT NULL,
    scope_kind TEXT NOT NULL,
    region_id INTEGER,
    location_id INTEGER,
    account_id TEXT,
    source TEXT NOT NULL,
    refresh_mode TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_order_snapshot_line (
    snapshot_id TEXT NOT NULL,
    order_id INTEGER NOT NULL,
    type_id INTEGER NOT NULL,
    is_buy_order INTEGER NOT NULL,
    price REAL NOT NULL,
    volume_remain REAL NOT NULL,
    volume_total REAL NOT NULL,
    min_volume REAL,
    order_range TEXT,
    issued_at TEXT,
    duration_days INTEGER,
    location_id INTEGER,
    region_id INTEGER,
    account_id TEXT,
    PRIMARY KEY (snapshot_id, order_id)
);

CREATE TABLE IF NOT EXISTS sync_scope_state (
    scope_key TEXT PRIMARY KEY,
    scope_type TEXT NOT NULL,
    region_id INTEGER,
    location_id INTEGER,
    type_id INTEGER,
    last_success_at TEXT,
    last_attempt_at TEXT,
    next_scheduled_at TEXT,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    coverage_marker TEXT
);

CREATE TABLE IF NOT EXISTS data_fetch_run (
    fetch_run_id TEXT PRIMARY KEY,
    fetch_kind TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    source TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    status TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS item_market_feature_snapshot (
    feature_run_id TEXT NOT NULL,
    type_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    location_id INTEGER,
    as_of_time TEXT NOT NULL,
    avg_daily_isk_volume_30d REAL,
    active_days_ratio_30d REAL,
    avg_daily_units_7d REAL,
    best_bid REAL,
    best_ask REAL,
    gross_spread_pct REAL,
    net_spread_pct REAL,
    cross_location_net_spread_pct REAL,
    price_zscore_30d REAL,
    price_percentile_30d REAL,
    volatility_30d REAL,
    estimated_exit_days_small_size REAL,
    PRIMARY KEY (feature_run_id, type_id, region_id, location_id)
);

CREATE TABLE IF NOT EXISTS item_risk_profile_snapshot (
    risk_run_id TEXT NOT NULL,
    type_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    trend_risk_level TEXT,
    inventory_risk_level TEXT,
    liquidity_risk_level TEXT,
    concentration_tag TEXT,
    safe_position_limit_isk REAL,
    safe_position_limit_units REAL,
    estimated_days_to_exit REAL,
    PRIMARY KEY (risk_run_id, type_id, region_id)
);

CREATE TABLE IF NOT EXISTS scan_run (
    scan_run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    market_scope TEXT NOT NULL,
    strategy_set TEXT NOT NULL,
    capital_budget_isk REAL NOT NULL,
    parameter_version TEXT NOT NULL,
    input_path TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    selected_candidate_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS opportunity_snapshot (
    scan_run_id TEXT NOT NULL,
    opportunity_id TEXT NOT NULL,
    strategy_type TEXT NOT NULL,
    type_id INTEGER NOT NULL,
    type_name TEXT NOT NULL,
    region_id INTEGER NOT NULL,
    buy_location_id INTEGER,
    sell_location_id INTEGER,
    entry_mode TEXT NOT NULL,
    exit_mode TEXT NOT NULL,
    unit_profit REAL NOT NULL,
    total_profit REAL NOT NULL,
    profit_margin REAL NOT NULL,
    capital_required REAL NOT NULL,
    tradable_units REAL NOT NULL,
    expected_turnover_days REAL NOT NULL,
    risk_adjusted_return REAL NOT NULL,
    decision_reason TEXT NOT NULL,
    PRIMARY KEY (scan_run_id, opportunity_id)
);
