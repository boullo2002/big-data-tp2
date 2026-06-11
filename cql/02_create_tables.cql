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
