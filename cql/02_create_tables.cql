USE cloud_analytics;

CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id text,
    usage_date date,
    service text,
    org_name text,
    plan_tier text,
    hq_region text,
    event_count bigint,
    requests double,
    total_value double,
    cpu_hours double,
    storage_gb_hours double,
    genai_tokens double,
    carbon_kg double,
    daily_cost_usd double,
    anomaly_event_count bigint,
    updated_at timestamp,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

CREATE TABLE IF NOT EXISTS org_service_cost_last_14d (
    org_id text,
    window_end_date date,
    total_cost_bucket int,
    service text,
    total_cost_usd double,
    requests double,
    event_count bigint,
    updated_at timestamp,
    PRIMARY KEY ((org_id, window_end_date), total_cost_bucket, service)
) WITH CLUSTERING ORDER BY (total_cost_bucket DESC, service ASC);
