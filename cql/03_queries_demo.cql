USE cloud_analytics;

-- Consulta 1:
-- Costos y requests diarios por org y servicio en rango de fechas.
SELECT
    org_id,
    usage_date,
    service,
    requests,
    cpu_hours,
    storage_gb_hours,
    daily_cost_usd,
    event_count
FROM org_daily_usage_by_service
WHERE org_id = 'org_4zw9xa3k'
  AND usage_date >= '2025-08-01'
  AND usage_date <= '2025-08-14';

-- Consulta 2:
-- Detalle de servicios y anomalias para una organizacion en una fecha.
SELECT
    org_id,
    usage_date,
    service,
    requests,
    cpu_hours,
    storage_gb_hours,
    genai_tokens,
    carbon_kg,
    daily_cost_usd,
    anomaly_event_count,
    event_count
FROM org_daily_usage_by_service
WHERE org_id = 'org_4zw9xa3k'
  AND usage_date = '2025-08-14';

-- Consulta extra TP1 - FinOps:
-- Revenue mensual por organizacion.
SELECT
    org_id,
    month,
    gross_revenue_usd,
    credits_usd,
    taxes_usd,
    net_revenue_usd,
    invoice_count
FROM revenue_by_org_month
WHERE org_id = 'org_4zw9xa3k'
  AND month >= '2025-06-01'
  AND month <= '2025-08-01';

-- Consulta extra TP1 - FinOps:
-- Eventos anomalos de costo por organizacion y rango de fechas.
SELECT
    org_id,
    usage_date,
    service,
    anomaly_event_count,
    negative_cost_event_count,
    high_cost_event_count,
    anomaly_cost_usd,
    max_event_cost_usd,
    min_event_cost_usd
FROM cost_anomaly_mart
WHERE org_id = 'org_4zw9xa3k'
  AND usage_date >= '2025-08-01'
  AND usage_date <= '2025-08-31';

-- Consulta extra TP1 - Soporte:
-- Tickets, SLA breach rate y CSAT promedio por organizacion y fecha.
SELECT
    org_id,
    ticket_date,
    category,
    severity,
    ticket_count,
    sla_breach_count,
    sla_breach_rate,
    avg_csat
FROM tickets_by_org_date
WHERE org_id = 'org_4zw9xa3k'
  AND ticket_date >= '2025-08-01'
  AND ticket_date <= '2025-08-31';

-- Consulta extra TP1 - Producto:
-- Consumo GenAI por organizacion y fecha.
SELECT
    org_id,
    usage_date,
    genai_tokens,
    estimated_token_cost_usd,
    requests,
    carbon_kg
FROM genai_tokens_by_org_date
WHERE org_id = 'org_4zw9xa3k'
  AND usage_date >= '2025-08-01'
  AND usage_date <= '2025-08-31';
