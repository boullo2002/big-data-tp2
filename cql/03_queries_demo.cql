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
-- Top-N servicios por costo acumulado en los ultimos 14 dias para una organizacion.
-- El bucket negativo/positivo se preagrega desde Spark para permitir lectura ordenada.
SELECT
    org_id,
    window_end_date,
    service,
    total_cost_usd,
    requests,
    event_count
FROM org_service_cost_last_14d
WHERE org_id = 'org_xaji0y6d'
  AND window_end_date = '2025-08-29'
LIMIT 5;
