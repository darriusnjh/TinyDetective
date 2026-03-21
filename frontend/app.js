const form = document.getElementById("investigation-form");
const sourceUrlsInput = document.getElementById("source-urls");
const comparisonSitesInput = document.getElementById("comparison-sites");
const resultsNode = document.getElementById("results");
const statusPill = document.getElementById("status-pill");
const progressText = document.getElementById("progress-text");
const configNote = document.getElementById("config-note");
const activityLogNode = document.getElementById("activity-log");
const reportTemplate = document.getElementById("report-template");
const matchTemplate = document.getElementById("match-template");

let pollTimer = null;

function parseLines(value) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function setStatus(status) {
  statusPill.textContent = status;
}

function renderActivityLog(entries) {
  if (!entries || entries.length === 0) {
    activityLogNode.innerHTML = '<p class="empty-state">No agent activity yet.</p>';
    return;
  }
  activityLogNode.innerHTML = entries
    .slice()
    .reverse()
    .map(
      (entry) => `
        <div class="activity-item">
          <strong>${entry.agent_name}</strong>${entry.message}<br />
          <span>${new Date(entry.timestamp).toLocaleTimeString()}</span>
          ${entry.source_url ? `<br /><span>${entry.source_url}</span>` : ""}
          ${
            entry.metadata && Object.keys(entry.metadata).length > 0
              ? `<div class="activity-meta">${JSON.stringify(entry.metadata)}</div>`
              : ""
          }
        </div>
      `
    )
    .join("");
}

function formatSourceProduct(product, error) {
  if (error) {
    return `Extraction failed: ${error}`;
  }
  if (!product) {
    return "No source product extracted.";
  }
  return `
    ${product.brand || "Unknown brand"} · ${product.product_name || "Unknown product"}<br />
    SKU: ${product.sku || "n/a"}<br />
    Model: ${product.model || "n/a"}<br />
    Price: ${product.currency || ""} ${product.price || "n/a"}<br />
    Material: ${product.material || "n/a"}<br />
    Features: ${(product.features || []).join(", ") || "n/a"}
  `;
}

function renderResults(payload) {
  resultsNode.innerHTML = "";
  if (!payload.reports || payload.reports.length === 0) {
    resultsNode.innerHTML = '<p class="empty-state">No reports yet.</p>';
    return;
  }

  payload.reports.forEach((report) => {
    const reportFragment = reportTemplate.content.cloneNode(true);
    reportFragment.querySelector(".report-summary").textContent = report.summary;
    reportFragment.querySelector(".report-source").innerHTML = `
      <strong>Source URL</strong><br />
      ${report.source_url}<br /><br />
      <strong>Extracted Product</strong><br />
      ${formatSourceProduct(report.extracted_source_product, report.error)}
    `;

    const matchesNode = reportFragment.querySelector(".matches");
    if (!report.top_matches || report.top_matches.length === 0) {
      matchesNode.innerHTML = '<p class="empty-state">No ranked matches returned.</p>';
    } else {
      report.top_matches.forEach((match) => {
        const matchFragment = matchTemplate.content.cloneNode(true);
        matchFragment.querySelector(".match-header").innerHTML = `
          <strong>${match.marketplace}</strong><br />
          <a href="${match.product_url}" target="_blank" rel="noreferrer">${match.product_url}</a>
        `;
        matchFragment.querySelector(".score-grid").innerHTML = `
          <div class="score-chip"><strong>Match Score</strong>${match.match_score}</div>
          <div class="score-chip"><strong>Counterfeit Risk</strong>${match.counterfeit_risk_score}</div>
          <div class="score-chip"><strong>Exact Match</strong>${match.is_exact_match ? "Yes" : "No"}</div>
        `;
        matchFragment.querySelector(".reason").textContent = match.reason;
        matchFragment.querySelector(".signals").innerHTML =
          match.suspicious_signals.length > 0
            ? match.suspicious_signals.map((signal) => `<span class="signal">${signal}</span>`).join("")
            : '<span class="empty-state">No suspicious signals flagged.</span>';
        matchFragment.querySelector(".evidence-list").innerHTML =
          match.evidence.length > 0
            ? match.evidence
                .map(
                  (item) => `
                    <div class="evidence-item">
                      <strong>${item.field}</strong> · ${item.note}<br />
                      Source: ${item.source_value ?? "n/a"}<br />
                      Candidate: ${item.candidate_value ?? "n/a"}<br />
                      Confidence: ${item.confidence}
                    </div>
                  `
                )
                .join("")
            : '<p class="empty-state">No evidence items returned.</p>';
        matchesNode.appendChild(matchFragment);
      });
    }

    reportFragment.querySelector(".agent-log-content").innerHTML = report.raw_agent_outputs
      .map(
        (task) => `
          <div class="agent-log-item">
            <strong>${task.agent_name}</strong> · ${task.status}<br />
            ${task.error ? `Error: ${task.error}<br />` : ""}
            <code>${JSON.stringify(task.output_payload, null, 2)}</code>
          </div>
        `
      )
      .join("");

    resultsNode.appendChild(reportFragment);
  });
}

async function fetchInvestigation(investigationId) {
  const response = await fetch(`/investigation/${investigationId}`);
  const payload = await response.json();
  setStatus(payload.status);
  progressText.textContent = `Investigation ${payload.investigation_id} is currently ${payload.status}.`;
  renderActivityLog(payload.activity_log || []);
  renderResults(payload);

  if (payload.status === "queued" || payload.status === "running") {
    pollTimer = window.setTimeout(() => fetchInvestigation(investigationId), 1200);
  } else if (pollTimer) {
    window.clearTimeout(pollTimer);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const source_urls = parseLines(sourceUrlsInput.value);
  const comparison_sites = parseLines(comparisonSitesInput.value);

  resultsNode.innerHTML = "";
  progressText.textContent = "Submitting investigation request.";
  setStatus("queued");
  renderActivityLog([]);

  const response = await fetch("/investigate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source_urls, comparison_sites }),
  });
  const payload = await response.json();
  fetchInvestigation(payload.investigation_id);
});

resultsNode.innerHTML =
  '<p class="empty-state">Enter source product URLs and run the live TinyFish-backed investigation.</p>';
renderActivityLog([]);

fetch("/config")
  .then((response) => response.json())
  .then((config) => {
    const stores = (config.ecommerce_store_urls || []).join(", ");
    const lines = [];
    if (config.brand_landing_page_url) {
      lines.push(`Configured brand landing page: ${config.brand_landing_page_url}`);
    }
    if (stores) {
      lines.push(`Configured store search targets: ${stores}`);
      if (!comparisonSitesInput.value.trim()) {
        comparisonSitesInput.value = (config.ecommerce_store_urls || []).join("\n");
      }
    }
    if (config.log_path) {
      lines.push(`Backend log file: ${config.log_path}`);
    }
    configNote.textContent = lines.join(" | ") || "No .env configuration loaded yet.";
  })
  .catch(() => {
    configNote.textContent = "Unable to load server configuration.";
  });
