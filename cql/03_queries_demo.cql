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
WHERE org_id = 'org_xaji0y6d'
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
WHERE org_id = 'org_xaji0y6d'
  AND usage_date = '2025-08-14';
