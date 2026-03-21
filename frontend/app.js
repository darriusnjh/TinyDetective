const form = document.getElementById("investigation-form");
const sourceUrlsInput = document.getElementById("source-urls");
const comparisonSitesInput = document.getElementById("comparison-sites");
const resultsNode = document.getElementById("results");
const pastRunsNode = document.getElementById("past-runs");
const statusPill = document.getElementById("status-pill");
const progressText = document.getElementById("progress-text");
const progressOverview = document.getElementById("progress-overview");
const progressTrack = document.getElementById("progress-track");
const progressFill = document.getElementById("progress-fill");
const configNote = document.getElementById("config-note");
const reportTemplate = document.getElementById("report-template");
const matchTemplate = document.getElementById("match-template");
const runButton = document.getElementById("run-button");

let pollTimer = null;
let currentInvestigationId = null;
let pastRunsCache = [];
const defaultRunButtonLabel = runButton.textContent;
const persistedInvestigationStorageKey = "tinydetective:last-investigation-id";
const progressStepDefinitions = [
  { key: "source_extraction", label: "Extract official product details" },
  { key: "candidate_discovery", label: "Search configured marketplaces" },
  { key: "product_comparison", label: "Compare candidate listings" },
  { key: "evidence", label: "Assemble supporting evidence" },
  { key: "ranking", label: "Rank suspicious matches" },
  { key: "research_summary", label: "Summarize the investigation" },
];
const progressStepIndex = Object.fromEntries(
  progressStepDefinitions.map((step, index) => [step.key, index])
);
const progressStepItems = Object.fromEntries(
  progressStepDefinitions.map((step) => [
    step.key,
    document.querySelector(`.progress-list [data-step="${step.key}"]`),
  ])
);
const statusLabels = {
  idle: "Idle",
  queued: "Queued",
  running: "Running",
  delayed: "Delayed",
  completed: "Completed",
  failed: "Failed",
};
const progressStateLabels = {
  pending: "Pending",
  queued: "Queued",
  running: "In Progress",
  delayed: "Delayed",
  completed: "Done",
  failed: "Failed",
};

function parseLines(value) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function setStatus(status) {
  const normalizedStatus = String(status || "idle").toLowerCase();
  statusPill.dataset.status = normalizedStatus;
  statusPill.textContent = statusLabels[normalizedStatus] || status;
}

function setSubmitting(isSubmitting) {
  runButton.disabled = isSubmitting;
  runButton.setAttribute("aria-busy", String(isSubmitting));
  runButton.textContent = isSubmitting ? "Starting Investigation..." : defaultRunButtonLabel;
}

function getPersistedInvestigationId() {
  try {
    return window.localStorage.getItem(persistedInvestigationStorageKey);
  } catch {
    return null;
  }
}

function persistInvestigationId(investigationId) {
  try {
    window.localStorage.setItem(persistedInvestigationStorageKey, investigationId);
  } catch {
    // Ignore local storage failures and keep the live in-memory flow working.
  }
}

function clearPersistedInvestigationId() {
  try {
    window.localStorage.removeItem(persistedInvestigationStorageKey);
  } catch {
    // Ignore local storage failures and keep the live in-memory flow working.
  }
  currentInvestigationId = null;
  renderPastRuns(pastRunsCache);
}

function selectInvestigation(investigationId) {
  currentInvestigationId = investigationId;
  persistInvestigationId(investigationId);
  renderPastRuns(pastRunsCache);
}

function loadInvestigation(investigationId) {
  if (!investigationId) {
    return;
  }
  if (pollTimer) {
    window.clearTimeout(pollTimer);
  }
  selectInvestigation(investigationId);
  fetchInvestigation(investigationId);
}

function sortMatchesByCounterfeitRisk(matches) {
  return [...(matches || [])].sort((left, right) => {
    const riskDelta = (right.counterfeit_risk_score || 0) - (left.counterfeit_risk_score || 0);
    if (riskDelta !== 0) {
      return riskDelta;
    }
    return (right.match_score || 0) - (left.match_score || 0);
  });
}

function sortPastRuns(runs) {
  return [...runs].sort((left, right) => {
    const leftTime = new Date(left.created_at).getTime();
    const rightTime = new Date(right.created_at).getTime();
    return rightTime - leftTime;
  });
}

function formatRunTimestamp(value) {
  if (!value) {
    return "Unknown time";
  }

  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) {
    return "Unknown time";
  }

  return timestamp.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatRunSource(sourceUrl) {
  if (!sourceUrl) {
    return {
      title: "Investigation",
      detail: "No source URL saved",
      full: "",
    };
  }

  try {
    const url = new URL(sourceUrl);
    const pathname = decodeURIComponent(url.pathname || "/").replace(/\/$/, "") || "/";
    return {
      title: url.hostname.replace(/^www\./, ""),
      detail: pathname === "/" ? "Homepage" : pathname,
      full: url.toString(),
    };
  } catch {
    return {
      title: sourceUrl,
      detail: "",
      full: sourceUrl,
    };
  }
}

function formatRunMeta(run, source) {
  if (run.error) {
    return run.error;
  }

  const parts = [];
  if (source.detail) {
    parts.push(source.detail);
  }
  parts.push(`${run.source_count || 0} source${run.source_count === 1 ? "" : "s"}`);
  return parts.join(" · ");
}

function createPastRunItem(run) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "past-run-item";
  button.dataset.investigationId = run.investigation_id;

  const header = document.createElement("div");
  header.className = "past-run-header";

  const status = document.createElement("span");
  status.className = "past-run-status";
  status.dataset.status = String(run.status || "queued").toLowerCase();

  const time = document.createElement("span");
  time.className = "past-run-time";

  header.append(status, time);

  const title = document.createElement("strong");
  title.className = "past-run-title";

  const meta = document.createElement("span");
  meta.className = "past-run-meta";

  button.append(header, title, meta);
  return button;
}

function renderPastRuns(runs) {
  if (!pastRunsNode) {
    return;
  }

  if (!runs || runs.length === 0) {
    pastRunsNode.innerHTML = '<p class="empty-state">No saved investigations yet.</p>';
    return;
  }

  const existingItems = new Map(
    [...pastRunsNode.querySelectorAll(".past-run-item")].map((node) => [node.dataset.investigationId, node])
  );

  runs.forEach((run) => {
    const investigationId = run.investigation_id;
    const source = formatRunSource(run.primary_source_url);

    let item = existingItems.get(investigationId);
    if (!item) {
      item = createPastRunItem(run);
    } else {
      existingItems.delete(investigationId);
    }

    item.classList.toggle("is-active", investigationId === currentInvestigationId);
    item.setAttribute("aria-pressed", investigationId === currentInvestigationId ? "true" : "false");
    item.title = source.full || source.title;
    item.querySelector(".past-run-status").dataset.status = String(run.status || "queued").toLowerCase();
    setTextContent(
      item.querySelector(".past-run-status"),
      statusLabels[String(run.status || "queued").toLowerCase()] || run.status
    );
    setTextContent(item.querySelector(".past-run-time"), formatRunTimestamp(run.created_at));
    setTextContent(item.querySelector(".past-run-title"), source.title);
    item.querySelector(".past-run-meta").dataset.tone = run.error ? "error" : "default";
    setTextContent(item.querySelector(".past-run-meta"), formatRunMeta(run, source));

    pastRunsNode.appendChild(item);
  });

  existingItems.forEach((node) => node.remove());
}

function upsertPastRun(run) {
  const nextRuns = [...pastRunsCache];
  const existingIndex = nextRuns.findIndex((item) => item.investigation_id === run.investigation_id);
  if (existingIndex === -1) {
    nextRuns.push(run);
  } else {
    nextRuns[existingIndex] = { ...nextRuns[existingIndex], ...run };
  }
  pastRunsCache = sortPastRuns(nextRuns);
  renderPastRuns(pastRunsCache);
}

function upsertPastRunFromInvestigation(payload) {
  const existingRun = pastRunsCache.find((item) => item.investigation_id === payload.investigation_id) || null;
  const nextRun = {
    investigation_id: payload.investigation_id,
    status: payload.status,
    primary_source_url:
      payload.reports?.[0]?.source_url || existingRun?.primary_source_url || null,
    source_count: payload.reports?.length || existingRun?.source_count || 0,
    error: payload.error || null,
    created_at: payload.created_at,
    updated_at: payload.updated_at,
  };
  upsertPastRun(nextRun);
}

async function refreshPastRuns() {
  if (!pastRunsNode) {
    return;
  }

  try {
    const response = await fetch("/investigations?limit=12");
    if (!response.ok) {
      throw new Error("Unable to load investigation history.");
    }
    pastRunsCache = sortPastRuns(await response.json());
    renderPastRuns(pastRunsCache);
  } catch (error) {
    if (pastRunsCache.length === 0) {
      pastRunsNode.innerHTML =
        '<p class="empty-state">Saved investigations could not be loaded right now.</p>';
    }
  }
}

function renderEmptyState(message) {
  resultsNode.innerHTML = `<p class="empty-state">${message}</p>`;
}

function setTextContent(node, value) {
  const nextValue = value ?? "";
  if (node.textContent !== nextValue) {
    node.textContent = nextValue;
  }
}

function setInnerHtml(node, value) {
  const nextValue = value ?? "";
  if (node.dataset.renderedHtml !== nextValue) {
    node.innerHTML = nextValue;
    node.dataset.renderedHtml = nextValue;
  }
}

function updateProgressUI({ overview, detail, percent, stepStates }) {
  progressOverview.textContent = overview;
  progressText.textContent = detail;
  progressFill.style.width = `${percent}%`;
  progressTrack.setAttribute("aria-valuenow", String(percent));

  progressStepDefinitions.forEach((step) => {
    const node = progressStepItems[step.key];
    const status = stepStates[step.key] || "pending";
    node.dataset.status = status;
    node.querySelector(".step-state").textContent = progressStateLabels[status] || status;
  });
}

function resetProgressTracking() {
  updateProgressUI({
    overview: "No investigation running yet.",
    detail: "Waiting for an investigation to start.",
    percent: 0,
    stepStates: Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "pending"])),
  });
}

function getActiveTask(report) {
  const tasks = report?.raw_agent_outputs || [];
  return (
    [...tasks].reverse().find((task) => task.status === "delayed") ||
    [...tasks].reverse().find((task) => task.status === "running") ||
    [...tasks].reverse().find((task) => task.status === "failed") ||
    [...tasks].reverse()[0] ||
    null
  );
}

function formatRelativeTime(value) {
  if (!value) {
    return null;
  }

  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) {
    return null;
  }

  const diffSeconds = Math.max(0, Math.round((Date.now() - timestamp.getTime()) / 1000));
  if (diffSeconds < 60) {
    return `${diffSeconds}s ago`;
  }
  if (diffSeconds < 3600) {
    return `${Math.round(diffSeconds / 60)}m ago`;
  }
  return `${Math.round(diffSeconds / 3600)}h ago`;
}

function describeProviderState(task) {
  if (!task) {
    return "";
  }

  const parts = [];
  if (task.provider_status) {
    parts.push(`TinyFish ${task.provider_status}`);
  }

  const heartbeat = formatRelativeTime(task.last_heartbeat_at);
  if (heartbeat) {
    parts.push(`heartbeat ${heartbeat}`);
  }

  const progress = formatRelativeTime(task.last_progress_at);
  if (progress && task.last_progress_at !== task.last_heartbeat_at) {
    parts.push(`last material update ${progress}`);
  }

  if (task.provider_run_id) {
    parts.push(`run ${String(task.provider_run_id).slice(0, 8)}`);
  }

  return parts.join(" · ");
}

function deriveReportStepStates(report) {
  const tasks = report?.raw_agent_outputs || [];

  return Object.fromEntries(
    progressStepDefinitions.map((step, stepIndex) => {
      const matchingTasks = tasks.filter((task) => task.agent_name === step.key);
      const laterStepStarted = tasks.some(
        (task) => (progressStepIndex[task.agent_name] ?? -1) > stepIndex
      );

      if (matchingTasks.some((task) => task.status === "failed")) {
        return [step.key, "failed"];
      }
      if (matchingTasks.some((task) => task.status === "delayed")) {
        return [step.key, "delayed"];
      }
      if (matchingTasks.some((task) => task.status === "running")) {
        return [step.key, "running"];
      }
      if (matchingTasks.length > 0 && matchingTasks.every((task) => task.status === "completed")) {
        return [step.key, "completed"];
      }
      if (laterStepStarted) {
        return [step.key, "completed"];
      }
      if (tasks.length === 0 && stepIndex === 0) {
        return [step.key, "queued"];
      }
      return [step.key, "pending"];
    })
  );
}

function getActiveReportIndex(reports) {
  const runningIndex = reports.findIndex((report) =>
    (report.raw_agent_outputs || []).some((task) => ["running", "delayed"].includes(task.status))
  );
  if (runningIndex !== -1) {
    return runningIndex;
  }

  const failedIndex = reports.findIndex((report) =>
    report.error || (report.raw_agent_outputs || []).some((task) => task.status === "failed")
  );
  if (failedIndex !== -1) {
    return failedIndex;
  }

  const nextQueuedIndex = reports.findIndex((report) => (report.raw_agent_outputs || []).length === 0);
  if (nextQueuedIndex === 0) {
    return 0;
  }
  if (nextQueuedIndex > 0) {
    return nextQueuedIndex - 1;
  }

  return Math.max(reports.length - 1, 0);
}

function isReportComplete(report) {
  const stepStates = deriveReportStepStates(report);
  return progressStepDefinitions.every((step) => stepStates[step.key] === "completed");
}

function calculateProgressPercent(reports, investigationStatus) {
  if (!reports.length) {
    return investigationStatus === "queued" ? 4 : 0;
  }

  const totalUnits = reports.length * progressStepDefinitions.length;
  let completedUnits = 0;

  reports.forEach((report) => {
    const stepStates = deriveReportStepStates(report);
    completedUnits += progressStepDefinitions.filter(
      (step) => stepStates[step.key] === "completed"
    ).length;
    if (
      progressStepDefinitions.some((step) =>
        ["running", "delayed"].includes(stepStates[step.key])
      )
    ) {
      completedUnits += 0.5;
    }
  });

  if (investigationStatus === "completed") {
    return 100;
  }

  return Math.max(0, Math.min(99, Math.round((completedUnits / totalUnits) * 100)));
}

function renderProgressTracking(payload) {
  const reports = payload.reports || [];
  const activeReport = reports[getActiveReportIndex(reports)] || null;
  const activeTask = getActiveTask(activeReport);
  const activeStepStates = activeReport
    ? deriveReportStepStates(activeReport)
    : Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "pending"]));
  const activeStep =
    progressStepDefinitions.find((step) => activeStepStates[step.key] === "delayed") ||
    progressStepDefinitions.find((step) => activeStepStates[step.key] === "running") ||
    progressStepDefinitions.find((step) => activeStepStates[step.key] === "failed") ||
    progressStepDefinitions.find((step) => activeStepStates[step.key] === "queued") ||
    progressStepDefinitions.find((step) => activeStepStates[step.key] === "pending");
  const completedReports = reports.filter(isReportComplete).length;
  const sourcePosition = activeReport ? getActiveReportIndex(reports) + 1 : 0;
  const totalSources = reports.length;

  let overview = "No investigation running yet.";
  let detail = "Waiting for an investigation to start.";

  if (payload.status === "queued") {
    overview = totalSources > 0 ? `Queued · Source 1 of ${totalSources}` : "Queued";
    detail = activeReport?.summary || "Preparing the investigation context.";
  } else if (payload.status === "running") {
    overview =
      totalSources > 1
        ? `Source ${sourcePosition} of ${totalSources} · ${activeStep?.label || "Processing"}`
        : activeStep?.label || "Investigation in progress";
    detail = activeReport?.summary || "Investigation is in progress.";
  } else if (payload.status === "delayed") {
    overview =
      totalSources > 1
        ? `Source ${sourcePosition} of ${totalSources} · Taking longer than usual`
        : "Taking longer than usual";
    detail = activeReport?.summary || "TinyFish is still working on the active step.";
  } else if (payload.status === "completed") {
    overview =
      totalSources > 1
        ? `Completed ${completedReports} of ${totalSources} source investigations`
        : "Investigation completed";
    detail = activeReport?.summary || "The investigation finished successfully.";
  } else if (payload.status === "failed") {
    overview = "Investigation failed";
    detail = payload.error || activeReport?.error || "The investigation ended with an error.";
  }

  const providerState = describeProviderState(activeTask);
  if (providerState) {
    detail = `${detail} ${detail.endsWith(".") ? "" : "."} ${providerState}`;
  }

  updateProgressUI({
    overview,
    detail,
    percent: calculateProgressPercent(reports, payload.status),
    stepStates: activeStepStates,
  });
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

function getReportKey(report, index) {
  return `${index}:${report.source_url}`;
}

function createReportCard(reportKey) {
  const reportFragment = reportTemplate.content.cloneNode(true);
  const reportCard = reportFragment.querySelector(".report-card");
  reportCard.dataset.reportKey = reportKey;
  return reportCard;
}

function renderMatches(matchesNode, topMatches) {
  const sortedMatches = sortMatchesByCounterfeitRisk(topMatches);
  const matchesFingerprint = JSON.stringify(sortedMatches);
  if (matchesNode.dataset.renderedMatches === matchesFingerprint) {
    return;
  }

  matchesNode.dataset.renderedMatches = matchesFingerprint;
  matchesNode.innerHTML = "";

  if (!sortedMatches || sortedMatches.length === 0) {
    matchesNode.innerHTML = '<p class="empty-state">No ranked matches were returned.</p>';
    return;
  }

  sortedMatches.forEach((match) => {
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
        : '<span class="empty-state">No suspicious signals were flagged.</span>';
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

function createAgentLogItem(taskId) {
  const item = document.createElement("div");
  item.className = "agent-log-item";
  item.dataset.taskId = taskId;

  const header = document.createElement("div");
  header.className = "agent-log-head";
  const name = document.createElement("strong");
  name.className = "agent-log-name";
  const status = document.createElement("span");
  status.className = "agent-log-status";
  header.append(name, document.createTextNode(" · "), status);

  const provider = document.createElement("div");
  provider.className = "agent-log-provider";

  const error = document.createElement("div");
  error.className = "agent-log-error";

  const output = document.createElement("code");
  output.className = "agent-log-output";

  item.append(header, provider, error, output);
  return item;
}

function renderAgentLog(agentLogContent, tasks) {
  const existingItems = new Map(
    [...agentLogContent.querySelectorAll(".agent-log-item")].map((node) => [node.dataset.taskId, node])
  );

  tasks.forEach((task) => {
    let item = existingItems.get(task.task_id);
    if (!item) {
      item = createAgentLogItem(task.task_id);
    } else {
      existingItems.delete(task.task_id);
    }

    setTextContent(item.querySelector(".agent-log-name"), task.agent_name);
    setTextContent(item.querySelector(".agent-log-status"), task.status);

    const providerState = describeProviderState(task);
    const providerNode = item.querySelector(".agent-log-provider");
    setTextContent(providerNode, providerState);
    providerNode.hidden = !providerState;

    const errorNode = item.querySelector(".agent-log-error");
    const errorText = task.error ? `Error: ${task.error}` : "";
    setTextContent(errorNode, errorText);
    errorNode.hidden = !errorText;

    setTextContent(
      item.querySelector(".agent-log-output"),
      JSON.stringify(task.output_payload, null, 2)
    );

    agentLogContent.appendChild(item);
  });

  existingItems.forEach((node) => node.remove());
}

function updateReportCard(reportCard, report) {
  setTextContent(reportCard.querySelector(".report-summary"), report.summary);
  setInnerHtml(
    reportCard.querySelector(".report-source"),
    `
      <strong>Source URL</strong><br />
      ${report.source_url}<br /><br />
      <strong>Extracted Product</strong><br />
      ${formatSourceProduct(report.extracted_source_product, report.error)}
    `
  );

  renderMatches(reportCard.querySelector(".matches"), report.top_matches || []);
  renderAgentLog(reportCard.querySelector(".agent-log-content"), report.raw_agent_outputs || []);
}

function renderResults(payload) {
  const visibleReports = (payload.reports || []).filter(
    (report) =>
      (report.raw_agent_outputs || []).length > 0 ||
      Boolean(report.extracted_source_product) ||
      (report.top_matches || []).length > 0 ||
      Boolean(report.error)
  );

  if (visibleReports.length === 0) {
    renderEmptyState("No investigation reports are available yet.");
    return;
  }

  const topLevelEmptyState = [...resultsNode.children].find((child) =>
    child.classList.contains("empty-state")
  );
  if (topLevelEmptyState) {
    topLevelEmptyState.remove();
  }

  const existingCards = new Map(
    [...resultsNode.querySelectorAll(".report-card")].map((node) => [node.dataset.reportKey, node])
  );
  const nextKeys = new Set();

  visibleReports.forEach((report, index) => {
    const reportKey = getReportKey(report, index);
    nextKeys.add(reportKey);

    let reportCard = existingCards.get(reportKey);
    if (!reportCard) {
      reportCard = createReportCard(reportKey);
    }

    updateReportCard(reportCard, report);
    resultsNode.appendChild(reportCard);
  });

  existingCards.forEach((node, key) => {
    if (!nextKeys.has(key)) {
      node.remove();
    }
  });
}

async function fetchInvestigation(investigationId) {
  try {
    const response = await fetch(`/investigation/${investigationId}`);
    if (!response.ok) {
      if (response.status === 404) {
        clearPersistedInvestigationId();
        throw new Error("The saved investigation was not found.");
      }
      throw new Error("Unable to refresh the investigation state.");
    }

    const payload = await response.json();
    selectInvestigation(payload.investigation_id);
    upsertPastRunFromInvestigation(payload);
    setStatus(payload.status);
    renderProgressTracking(payload);
    renderResults(payload);

    if (["queued", "running", "delayed"].includes(payload.status)) {
      pollTimer = window.setTimeout(() => fetchInvestigation(investigationId), 1200);
    } else if (pollTimer) {
      window.clearTimeout(pollTimer);
      refreshPastRuns();
    }
  } catch (error) {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
    }
    setStatus("failed");
    updateProgressUI({
      overview: "Progress unavailable",
      detail: error.message,
      percent: 0,
      stepStates: Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "failed"])),
    });
    renderEmptyState("The investigation state could not be refreshed. Try again in a moment.");
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const source_urls = parseLines(sourceUrlsInput.value);
  const comparison_sites = parseLines(comparisonSitesInput.value);

  if (source_urls.length === 0) {
    setStatus("idle");
    updateProgressUI({
      overview: "Official product URL required",
      detail: "Add at least one official product page URL to begin.",
      percent: 0,
      stepStates: Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "pending"])),
    });
    renderEmptyState("Add one or more official product page URLs, one per line.");
    sourceUrlsInput.focus();
    return;
  }

  if (pollTimer) {
    window.clearTimeout(pollTimer);
  }

  setStatus("queued");
  updateProgressUI({
    overview: "Submitting investigation request",
    detail: "Creating the investigation and preparing live progress updates.",
    percent: 4,
    stepStates: Object.fromEntries(
      progressStepDefinitions.map((step, index) => [step.key, index === 0 ? "queued" : "pending"])
    ),
  });
  renderEmptyState("Starting a live investigation and preparing the first result set.");
  setSubmitting(true);

  try {
    const response = await fetch("/investigate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_urls, comparison_sites }),
    });
    if (!response.ok) {
      throw new Error("Unable to start the investigation.");
    }

    const payload = await response.json();
    await refreshPastRuns();
    loadInvestigation(payload.investigation_id);
  } catch (error) {
    setStatus("failed");
    updateProgressUI({
      overview: "Investigation failed to start",
      detail: error.message,
      percent: 0,
      stepStates: Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "failed"])),
    });
    renderEmptyState("The investigation could not be started. Check the backend and try again.");
  } finally {
    setSubmitting(false);
  }
});

if (pastRunsNode) {
  pastRunsNode.addEventListener("click", (event) => {
    const button = event.target.closest(".past-run-item");
    if (!button) {
      return;
    }
    loadInvestigation(button.dataset.investigationId);
  });
}

setStatus("idle");
resetProgressTracking();
renderEmptyState("Add official product page URLs to compare them against live marketplace listings.");

currentInvestigationId = getPersistedInvestigationId();
refreshPastRuns();
if (currentInvestigationId) {
  setStatus("queued");
  updateProgressUI({
    overview: "Restoring previous investigation",
    detail: "Reloading the latest saved investigation state.",
    percent: 4,
    stepStates: Object.fromEntries(
      progressStepDefinitions.map((step, index) => [step.key, index === 0 ? "queued" : "pending"])
    ),
  });
  renderEmptyState("Restoring the latest saved investigation state.");
  fetchInvestigation(currentInvestigationId);
}

fetch("/config")
  .then((response) => response.json())
  .then((config) => {
    const stores = (config.ecommerce_store_urls || []).join(", ");
    const lines = [];
    if (config.brand_landing_page_url) {
      lines.push(`Brand home: ${config.brand_landing_page_url}`);
    }
    if (stores) {
      lines.push(`Default marketplace targets: ${stores}`);
      if (!comparisonSitesInput.value.trim()) {
        comparisonSitesInput.value = (config.ecommerce_store_urls || []).join("\n");
      }
    }
    configNote.textContent =
      lines.join(" • ") ||
      "Environment defaults are not loaded yet. You can still enter source pages and marketplace targets manually.";
  })
  .catch(() => {
    configNote.textContent =
      "Environment defaults could not be loaded. Manual inputs still work.";
  });
