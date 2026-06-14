USE cloud_analytics;

-- ============================================================
-- Consultas de la consigna (1-5)
-- ============================================================

-- Consulta 1 (consigna):
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

-- Consulta 2 (consigna):
-- Top-N servicios por costo acumulado en los últimos 14 días.
SELECT
    org_id,
    service,
    total_cost_usd,
    event_count,
    requests
FROM org_service_cost_14d
WHERE org_id = 'org_4zw9xa3k'
LIMIT 5;

-- Consulta 3 (consigna):
-- Evolución de tickets críticos y SLA breach por día (últimos 30 días).
SELECT
    org_id,
    ticket_date,
    ticket_count,
    resolved_ticket_count,
    open_ticket_count,
    sla_breach_count,
    sla_breach_rate,
    avg_csat
FROM tickets_critical_by_org_date
WHERE org_id = 'org_4zw9xa3k'
  AND ticket_date >= '2025-08-01'
  AND ticket_date <= '2025-08-31';

-- Consulta 4 (consigna):
-- Revenue mensual con créditos/impuestos aplicados (normalizado a USD).
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

-- Consulta 5 (consigna):
-- Tokens GenAI y costo estimado por día.
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

-- ============================================================
-- Consultas extra (6-7)
-- ============================================================

-- Consulta 6 (extra):
-- Detalle de servicios y anomalías para una organización en una fecha puntual.
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

-- Consulta 7 (extra):
-- Eventos anómalos de costo por organización y rango de fechas.
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
