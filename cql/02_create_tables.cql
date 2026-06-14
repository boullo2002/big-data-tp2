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

CREATE TABLE IF NOT EXISTS revenue_by_org_month (
    org_id text,
    month date,
    org_name text,
    industry text,
    plan_tier text,
    hq_region text,
    lifecycle_stage text,
    invoice_count bigint,
    gross_revenue_usd double,
    credits_usd double,
    taxes_usd double,
    net_revenue_usd double,
    updated_at timestamp,
    PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

CREATE TABLE IF NOT EXISTS cost_anomaly_mart (
    org_id text,
    usage_date date,
    service text,
    org_name text,
    plan_tier text,
    hq_region text,
    anomaly_event_count bigint,
    negative_cost_event_count bigint,
    high_cost_event_count bigint,
    anomaly_cost_usd double,
    max_event_cost_usd double,
    min_event_cost_usd double,
    updated_at timestamp,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

CREATE TABLE IF NOT EXISTS tickets_by_org_date (
    org_id text,
    ticket_date date,
    category text,
    severity text,
    org_name text,
    industry text,
    plan_tier text,
    hq_region text,
    ticket_count bigint,
    resolved_ticket_count bigint,
    open_ticket_count bigint,
    sla_breach_count bigint,
    sla_breach_rate double,
    avg_csat double,
    updated_at timestamp,
    PRIMARY KEY ((org_id), ticket_date, category, severity)
) WITH CLUSTERING ORDER BY (ticket_date DESC, category ASC, severity ASC);

CREATE TABLE IF NOT EXISTS org_service_cost_14d (
    org_id text,
    total_cost_usd double,
    service text,
    org_name text,
    plan_tier text,
    hq_region text,
    event_count bigint,
    requests double,
    updated_at timestamp,
    PRIMARY KEY ((org_id), total_cost_usd, service)
) WITH CLUSTERING ORDER BY (total_cost_usd DESC, service ASC);

CREATE TABLE IF NOT EXISTS tickets_critical_by_org_date (
    org_id text,
    ticket_date date,
    org_name text,
    industry text,
    plan_tier text,
    hq_region text,
    ticket_count bigint,
    resolved_ticket_count bigint,
    open_ticket_count bigint,
    sla_breach_count bigint,
    sla_breach_rate double,
    avg_csat double,
    updated_at timestamp,
    PRIMARY KEY ((org_id), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
    org_id text,
    usage_date date,
    org_name text,
    plan_tier text,
    hq_region text,
    event_count bigint,
    requests double,
    genai_tokens double,
    estimated_token_cost_usd double,
    carbon_kg double,
    updated_at timestamp,
    PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);
