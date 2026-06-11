const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
const number = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const pct = new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 });

const colors = ["#0f6f5c", "#275f92", "#a33d2e", "#c7922f", "#5d4b82", "#6f5d42"];

function sum(rows, key) {
  return rows.reduce((acc, row) => acc + Number(row[key] || 0), 0);
}

function groupSum(rows, keys, metric) {
  const grouped = new Map();
  rows.forEach((row) => {
    const id = keys.map((key) => row[key]).join("|");
    const current = grouped.get(id) || Object.fromEntries(keys.map((key) => [key, row[key]]));
    current[metric] = Number(current[metric] || 0) + Number(row[metric] || 0);
    grouped.set(id, current);
  });
  return [...grouped.values()];
}

function drawLineChart(canvas, series, options = {}) {
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  const pad = { left: 64, right: 24, top: 28, bottom: 46 };
  ctx.clearRect(0, 0, width, height);

  const labels = [...new Set(series.flatMap((item) => item.points.map((point) => point.x)))].sort();
  const values = series.flatMap((item) => item.points.map((point) => point.y));
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const plotW = width - pad.left - pad.right;
  const plotH = height - pad.top - pad.bottom;

  ctx.strokeStyle = "#d9d2bf";
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let i = 0; i <= 4; i += 1) {
    const y = pad.top + (plotH / 4) * i;
    ctx.moveTo(pad.left, y);
    ctx.lineTo(width - pad.right, y);
  }
  ctx.stroke();

  ctx.fillStyle = "#65716a";
  ctx.font = "18px Georgia";
  ctx.fillText(options.label || "", pad.left, 20);
  ctx.font = "12px Menlo";
  labels.filter((_, index) => index % Math.ceil(labels.length / 6 || 1) === 0).forEach((label) => {
    const x = pad.left + (labels.indexOf(label) / Math.max(labels.length - 1, 1)) * plotW;
    ctx.fillText(label.slice(5), x - 18, height - 16);
  });

  series.forEach((item, index) => {
    ctx.strokeStyle = colors[index % colors.length];
    ctx.lineWidth = 3;
    ctx.beginPath();
    item.points.forEach((point, pointIndex) => {
      const x = pad.left + (labels.indexOf(point.x) / Math.max(labels.length - 1, 1)) * plotW;
      const y = pad.top + plotH - ((point.y - min) / Math.max(max - min, 1)) * plotH;
      if (pointIndex === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  });
}

function drawBarChart(canvas, rows, labelKey, valueKey, options = {}) {
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  const pad = { left: 62, right: 22, top: 24, bottom: 54 };
  ctx.clearRect(0, 0, width, height);
  const max = Math.max(...rows.map((row) => Number(row[valueKey] || 0)), 1);
  const barW = (width - pad.left - pad.right) / Math.max(rows.length, 1);

  ctx.fillStyle = "#65716a";
  ctx.font = "18px Georgia";
  ctx.fillText(options.label || "", pad.left, 18);

  rows.forEach((row, index) => {
    const value = Number(row[valueKey] || 0);
    const h = ((height - pad.top - pad.bottom) * value) / max;
    const x = pad.left + index * barW + 8;
    const y = height - pad.bottom - h;
    ctx.fillStyle = colors[index % colors.length];
    ctx.fillRect(x, y, Math.max(barW - 16, 8), h);
    ctx.fillStyle = "#65716a";
    ctx.font = "12px Menlo";
    ctx.save();
    ctx.translate(x + 6, height - 18);
    ctx.rotate(-0.55);
    ctx.fillText(String(row[labelKey]).slice(0, 10), 0, 0);
    ctx.restore();
  });
}

function renderTables(data, orgId) {
  const tickets = data.tickets_by_org_date
    .filter((row) => row.org_id === orgId)
    .sort((a, b) => b.ticket_date.localeCompare(a.ticket_date))
    .slice(0, 12);
  document.querySelector("#tickets-table").innerHTML = tickets
    .map((row) => `<tr><td>${row.ticket_date}</td><td>${row.ticket_count}</td><td>${pct.format(row.sla_breach_rate || 0)}</td><td>${Number(row.avg_csat || 0).toFixed(2)}</td></tr>`)
    .join("");

  const anomalies = data.cost_anomaly_mart
    .sort((a, b) => Number(b.anomaly_cost_usd || 0) - Number(a.anomaly_cost_usd || 0))
    .slice(0, 14);
  document.querySelector("#anomaly-table").innerHTML = anomalies
    .map((row) => `<tr><td>${row.org_id}</td><td>${row.usage_date}</td><td>${row.service}</td><td>${row.anomaly_event_count}</td><td>${money.format(row.anomaly_cost_usd || 0)}</td></tr>`)
    .join("");
}

function render(data) {
  const orgs = [...new Set(data.org_daily_usage_by_service.map((row) => row.org_id))].sort();
  const orgSelect = document.querySelector("#org-select");
  orgSelect.innerHTML = orgs.map((org) => `<option value="${org}">${org}</option>`).join("");
  orgSelect.value = data.default_org || orgs[0];

  document.querySelector("#kpi-cost").textContent = money.format(sum(data.org_daily_usage_by_service, "daily_cost_usd"));
  document.querySelector("#kpi-revenue").textContent = money.format(sum(data.revenue_by_org_month, "net_revenue_usd"));
  document.querySelector("#kpi-tickets").textContent = number.format(sum(data.tickets_by_org_date, "ticket_count"));
  document.querySelector("#kpi-tokens").textContent = number.format(sum(data.genai_tokens_by_org_date, "genai_tokens"));

  function renderOrg() {
    const orgId = orgSelect.value;
    const costs = groupSum(
      data.org_daily_usage_by_service.filter((row) => row.org_id === orgId),
      ["usage_date", "service"],
      "daily_cost_usd",
    );
    const services = [...new Set(costs.map((row) => row.service))].sort();
    drawLineChart(
      document.querySelector("#cost-chart"),
      services.map((service) => ({
        name: service,
        points: costs.filter((row) => row.service === service).map((row) => ({ x: row.usage_date, y: row.daily_cost_usd })),
      })),
      { label: orgId },
    );

    const revenue = groupSum(
      data.revenue_by_org_month.filter((row) => row.org_id === orgId),
      ["month"],
      "net_revenue_usd",
    ).sort((a, b) => a.month.localeCompare(b.month));
    drawBarChart(document.querySelector("#revenue-chart"), revenue, "month", "net_revenue_usd", { label: orgId });

    const genai = groupSum(
      data.genai_tokens_by_org_date.filter((row) => row.org_id === orgId),
      ["usage_date"],
      "genai_tokens",
    ).sort((a, b) => a.usage_date.localeCompare(b.usage_date));
    drawBarChart(document.querySelector("#genai-chart"), genai.slice(-18), "usage_date", "genai_tokens", { label: orgId });

    const tickets = data.tickets_by_org_date.filter((row) => row.org_id === orgId);
    const totalTickets = sum(tickets, "ticket_count");
    const breach = sum(tickets, "sla_breach_count");
    const csatRows = tickets.filter((row) => row.avg_csat);
    const avgCsat = csatRows.reduce((acc, row) => acc + Number(row.avg_csat), 0) / Math.max(csatRows.length, 1);
    document.querySelector("#sla-rate").textContent = pct.format(breach / Math.max(totalTickets, 1));
    document.querySelector("#avg-csat").textContent = avgCsat.toFixed(2);

    renderTables(data, orgId);
  }

  orgSelect.addEventListener("change", renderOrg);
  renderOrg();
}

fetch("data/marts.json")
  .then((response) => response.json())
  .then(render)
  .catch((error) => {
    document.body.innerHTML = `<pre>No se pudo cargar dashboard/data/marts.json\n${error}</pre>`;
  });
