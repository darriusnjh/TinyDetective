const body = document.body;
const promptComposer = document.getElementById("prompt-composer");
const form = document.getElementById("investigation-form");
const sourceUrlsInput = document.getElementById("source-urls");
const comparisonSitesInput = document.getElementById("comparison-sites");
const resultsNode = document.getElementById("results");
const pastRunsNode = document.getElementById("past-runs");
const historyDropdown = document.getElementById("history-dropdown");
const historyButton = document.getElementById("history-button");
const statusPill = document.getElementById("status-pill");
const progressText = document.getElementById("progress-text");
const progressOverview = document.getElementById("progress-overview");
const progressTrack = document.getElementById("progress-track");
const progressFill = document.getElementById("progress-fill");
const configNote = document.getElementById("config-note");
const reportTemplate = document.getElementById("report-template");
const matchTemplate = document.getElementById("match-template");
const runButton = document.getElementById("run-button");
const timelineSourceUrl = document.getElementById("timeline-source-url");
const timelineSourceLink = document.getElementById("timeline-source-link");
const timelineSourceFrame = document.getElementById("timeline-source-frame");
const timelineSourceMeta = document.getElementById("timeline-source-meta");
const timelineSearchLog = document.getElementById("timeline-search-log");
const timelineCandidateStream = document.getElementById("timeline-candidate-stream");
const timelineSignalGraph = document.getElementById("timeline-signal-graph");
const timelineAnalysisLog = document.getElementById("timeline-analysis-log");
const timelineRankingList = document.getElementById("timeline-ranking-list");
const generateReportButton = document.getElementById("generate-report-button");
const newInvestigationButton = document.getElementById("new-investigation-button");
const reportPdfFrame = document.getElementById("report-pdf-frame");
const reportMeta = document.getElementById("report-meta");
const reportNote = document.getElementById("report-note");
const reportBackButton = document.getElementById("report-back-button");
const reportOpenButton = document.getElementById("report-open-button");
const reportNewButton = document.getElementById("report-new-button");
const timelineTrack = document.getElementById("progress-list");
const timelineNotes = {
  source: document.getElementById("timeline-source-note"),
  search: document.getElementById("timeline-search-note"),
  candidates: document.getElementById("timeline-candidates-note"),
  analysis: document.getElementById("timeline-analysis-note"),
  ranking: document.getElementById("timeline-ranking-note"),
};

let pollTimer = null;
let currentInvestigationId = null;
let pastRunsCache = [];
let currentPhase = body.dataset.phase || "prompt";
let lastSubmittedSourceUrl = "";
let activeTimelineStage = "source";
let latestInvestigationPayload = null;
let appConfig = null;
let currentReportDocumentUrl = null;
let reportGenerationInFlight = false;

const defaultRunButtonLabel = runButton.textContent;
const defaultGenerateReportButtonLabel = generateReportButton?.textContent || "Generate report";
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
const timelineStageDefinitions = [
  { key: "source", label: "Source Page" },
  { key: "search", label: "Live Search Behavior" },
  { key: "candidates", label: "Candidate Intake" },
  { key: "analysis", label: "Reasoning Graph" },
  { key: "ranking", label: "Ranking Ladder" },
];
const timelineStageItems = Object.fromEntries(
  timelineStageDefinitions.map((stage) => [
    stage.key,
    document.querySelector(`[data-timeline-step="${stage.key}"]`),
  ])
);
const timelineRailItems = Object.fromEntries(
  timelineStageDefinitions.map((stage) => [
    stage.key,
    document.querySelector(`[data-timeline-rail="${stage.key}"]`),
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
const timelineStateLabels = {
  pending: "Pending",
  queued: "Queued",
  running: "Live",
  delayed: "Delayed",
  completed: "Ready",
  failed: "Failed",
};

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => {
    const entities = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    };
    return entities[character] || character;
  });
}

function formatHostname(value) {
  if (!value) {
    return "Unknown source";
  }

  try {
    return new URL(value).hostname.replace(/^www\./, "");
  } catch {
    return String(value);
  }
}

function formatCompactCurrency(value, currency) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "Price unavailable";
  }

  try {
    return new Intl.NumberFormat([], {
      style: "currency",
      currency: currency || "USD",
      maximumFractionDigits: 0,
    }).format(Number(value));
  } catch {
    return `${currency || ""} ${value}`.trim();
  }
}

function formatElapsedSeconds(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return null;
  }

  const seconds = Math.max(1, Math.round(Number(value)));
  if (seconds < 60) {
    return `${seconds}s`;
  }
  if (seconds < 3600) {
    return `${Math.round(seconds / 60)}m`;
  }
  return `${Math.round(seconds / 3600)}h`;
}

function normalizeScore(value) {
  const numericValue = Number(value) || 0;
  if (numericValue > 1) {
    return Math.max(0, Math.min(numericValue / 10, 1));
  }
  return Math.max(0, Math.min(numericValue, 1));
}

function getRiskColor(value) {
  const normalizedValue = normalizeScore(value);
  if (normalizedValue >= 0.75) {
    return "hsl(7 72% 46%)";
  }
  if (normalizedValue >= 0.45) {
    return "hsl(35 82% 46%)";
  }
  return "hsl(145 58% 38%)";
}

function humanizeFieldName(value) {
  return String(value || "field")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function humanizeSignal(signal) {
  const signalMap = {
    suspiciously_low_price: "The listing is priced materially below the official source price.",
    brand_mismatch: "The brand information does not align with the official product.",
    copied_description_with_discount_pricing:
      "The listing appears to reuse official product copy while also discounting heavily.",
  };

  return signalMap[signal] || humanizeFieldName(signal);
}

function formatNaturalList(items) {
  const values = [...new Set((items || []).filter(Boolean))];
  if (values.length === 0) {
    return "";
  }
  if (values.length === 1) {
    return values[0];
  }
  if (values.length === 2) {
    return `${values[0]} and ${values[1]}`;
  }
  return `${values.slice(0, -1).join(", ")}, and ${values[values.length - 1]}`;
}

function getRiskReasonLines(match) {
  const lines = [];
  const evidence = match.evidence || [];
  const priceEvidence = evidence.find((item) => item.field === "price");
  const descriptionEvidence = evidence.find((item) => item.field === "description");

  (match.suspicious_signals || []).forEach((signal) => {
    lines.push(humanizeSignal(signal));
  });

  if (priceEvidence && !lines.some((line) => line.toLowerCase().includes("priced materially below"))) {
    lines.push(priceEvidence.note);
  }

  if (
    descriptionEvidence &&
    !lines.some((line) => line.toLowerCase().includes("reuse official product copy"))
  ) {
    lines.push(descriptionEvidence.note);
  }

  if (lines.length === 0) {
    if (normalizeScore(match.counterfeit_risk_score) < 0.35) {
      lines.push("Few direct counterfeit indicators were detected in the captured evidence.");
    } else {
      lines.push(match.reason || "The backend did not return a more specific counterfeit-risk rationale.");
    }
  }

  return [...new Set(lines)];
}

function getMatchReasonLines(match) {
  const evidence = match.evidence || [];
  const matchedFields = evidence
    .filter((item) => /matches between source and candidate/i.test(item.note))
    .map((item) => humanizeFieldName(item.field));
  const mismatchedFields = evidence
    .filter((item) => /does not match between source and candidate/i.test(item.note))
    .map((item) => humanizeFieldName(item.field));
  const lines = [];

  if (matchedFields.length > 0) {
    lines.push(`Aligned fields: ${formatNaturalList(matchedFields)}.`);
  }

  if (mismatchedFields.length > 0) {
    lines.push(`Mismatched fields: ${formatNaturalList(mismatchedFields)}.`);
  }

  if (normalizeScore(match.match_score) < 0.5) {
    if (matchedFields.length <= 1) {
      lines.push("Too few structured attributes aligned strongly with the official product.");
    }
    if (mismatchedFields.length === 0 && matchedFields.length === 0) {
      lines.push("The backend found only weak directional similarity rather than a strong structured match.");
    }
  } else if (normalizeScore(match.match_score) >= 0.75 && matchedFields.length > 0) {
    lines.push("Multiple structured attributes line up with the official product, which keeps the match score elevated.");
  }

  if (lines.length === 0) {
    lines.push(match.reason || "The backend did not return a more specific match rationale.");
  }

  return [...new Set(lines)];
}

function sanitizePlainText(value) {
  const raw = String(value ?? "");
  const normalized = typeof raw.normalize === "function" ? raw.normalize("NFKD") : raw;

  return normalized
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201c\u201d]/g, '"')
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/\u2026/g, "...")
    .replace(/[\u00b7\u2022]/g, " - ")
    .replace(/\u00a0/g, " ")
    .replace(/\u2122/g, "(TM)")
    .replace(/\u00ae/g, "(R)")
    .replace(/\u00a9/g, "(C)")
    .replace(/[【〔〖〘［]/g, "[")
    .replace(/[】〕〗〙］]/g, "]")
    .replace(/[（]/g, "(")
    .replace(/[）]/g, ")")
    .replace(/[：]/g, ":")
    .replace(/[，]/g, ",")
    .replace(/[；]/g, ";")
    .replace(/[／]/g, "/")
    .replace(/[｜]/g, "|")
    .replace(/[＋]/g, "+")
    .replace(/[＝]/g, "=")
    .replace(/[％]/g, "%")
    .replace(/[＆]/g, "&")
    .replace(/[＃]/g, "#")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[\u0000-\u001f\u007f-\u009f]/g, " ")
    .replace(/[^\x20-\x7e]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function formatReportDate(value) {
  if (!value) {
    return "Unavailable";
  }

  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) {
    return "Unavailable";
  }

  return timestamp.toLocaleString();
}

function getBrandWebsite(payload) {
  return (
    appConfig?.brand_landing_page_url ||
    payload?.reports?.[0]?.source_url ||
    lastSubmittedSourceUrl ||
    "Unavailable"
  );
}

function getSuggestedActionsForReport(report) {
  const rankedListings = getRankingSnapshots(report);
  const highRiskListings = rankedListings.filter((item) => normalizeScore(item.counterfeit_risk_score) >= 0.75);
  const mediumRiskListings = rankedListings.filter((item) => {
    const score = normalizeScore(item.counterfeit_risk_score);
    return score >= 0.45 && score < 0.75;
  });
  const officialLikeListings = collectCompletedComparisons(report).filter(
    (item) => item.official_store_signals && item.official_store_signals.length > 0
  );

  if (highRiskListings.length > 0) {
    return [
      `Preserve evidence for the ${highRiskListings.length} highest-risk listing${highRiskListings.length === 1 ? "" : "s"} and capture screenshots before they change.`,
      "Escalate the top suspicious URLs to marketplace trust and safety or brand protection workflows for takedown review.",
      "Cross-check seller identity, price, and product attributes against authorized channels before enforcement.",
    ];
  }

  if (mediumRiskListings.length > 0) {
    return [
      "Queue the medium-risk listings for manual analyst review before any takedown request is sent.",
      "Compare seller metadata, pricing, and evidence notes against the official product page to confirm whether escalation is warranted.",
      "Continue monitoring search coverage in case stronger lookalikes appear in subsequent crawls.",
    ];
  }

  if (officialLikeListings.length > 0) {
    return [
      "Do not treat official-store-like listings as counterfeit without manual confirmation.",
      "Review the official-store signals first and separate those listings from enforcement queues.",
    ];
  }

  return [
    "No high-confidence counterfeit target was found in the ranked set; keep monitoring and rerun the investigation if new listings appear.",
    "Archive the collected evidence and search coverage as a baseline for future comparisons.",
  ];
}

function buildOperationalTrace(task) {
  const output = task.output_payload || {};
  const traceDetails = [];

  if (task.agent_name === "candidate_discovery") {
    traceDetails.push(
      `site: ${sanitizePlainText(
        output.comparison_site || task.input_payload?.comparison_site || "Unavailable"
      )}`
    );
    traceDetails.push(
      `query: ${sanitizePlainText(output.search_query || task.input_payload?.search_query || "Unavailable")}`
    );
    if (output.candidate_count !== undefined) {
      traceDetails.push(`candidate count: ${sanitizePlainText(output.candidate_count)}`);
    }
  }

  if (task.agent_name === "product_comparison" && output.comparison?.product_url) {
    traceDetails.push(`product: ${sanitizePlainText(output.comparison.product_url)}`);
  }

  if (task.agent_name === "ranking" && output.ranked_product_urls?.length) {
    traceDetails.push(`ranked URLs: ${sanitizePlainText(output.ranked_product_urls.length)}`);
  }

  if (task.error) {
    traceDetails.push(`error: ${sanitizePlainText(task.error)}`);
  }

  const providerState = describeProviderState(task);
  if (providerState) {
    traceDetails.push(`provider: ${sanitizePlainText(providerState)}`);
  }

  return traceDetails;
}

function getReportLimitations(report) {
  const gaps = [
    "This dossier captures observed URLs, extracted listing data, and heuristic comparison evidence only. It does not itself prove infringement or counterfeit authenticity as a legal conclusion.",
    "Trademark registrations, chain-of-title documents, prior enforcement history, and counsel-reviewed legal claims should be attached separately before filing a complaint or lawsuit.",
    "Screenshots, page captures, and test-buy evidence were not automatically preserved in this run and should be captured separately if platform reporting or litigation support requires them.",
  ];

  const missingSellerCount = getRankingSnapshots(report)
    .slice(0, 5)
    .filter((item) => !item.candidate_product?.seller_name).length;

  if (missingSellerCount > 0) {
    gaps.push(
      `${missingSellerCount} ranked listing${missingSellerCount === 1 ? "" : "s"} did not include seller identity in the captured data and may need manual follow-up.`
    );
  }

  return gaps;
}

function buildInvestigationPdfFallback(payload) {
  const jsPdfApi = window.jspdf?.jsPDF;
  if (!jsPdfApi) {
    throw new Error("The PDF renderer is not available in this browser session.");
  }

  const doc = new jsPdfApi({
    orientation: "portrait",
    unit: "pt",
    format: "letter",
    compress: true,
  });

  const pageWidth = doc.internal.pageSize.getWidth();
  const pageHeight = doc.internal.pageSize.getHeight();
  const margin = 40;
  const footerHeight = 24;
  const contentWidth = pageWidth - margin * 2;
  let cursorY = margin;

  const ensureSpace = (height = 18) => {
    if (cursorY + height <= pageHeight - margin - footerHeight) {
      return;
    }
    doc.addPage();
    cursorY = margin;
  };

  const drawRule = () => {
    ensureSpace(12);
    doc.setDrawColor(204, 198, 188);
    doc.setLineWidth(0.6);
    doc.line(margin, cursorY, pageWidth - margin, cursorY);
    cursorY += 10;
  };

  const addKicker = (text) => {
    ensureSpace(12);
    doc.setFont("helvetica", "bold");
    doc.setFontSize(8.5);
    doc.setTextColor(132, 124, 113);
    doc.text(sanitizePlainText(String(text || "").toUpperCase()), margin, cursorY);
    cursorY += 11;
  };

  const addHeading = (text, size = 18) => {
    doc.setFont("helvetica", "bold");
    doc.setFontSize(size);
    doc.setTextColor(54, 46, 39);
    const lines = doc.splitTextToSize(sanitizePlainText(text), contentWidth);
    ensureSpace(lines.length * (size + 2));
    doc.text(lines, margin, cursorY);
    cursorY += lines.length * (size + 2);
  };

  const addParagraph = (text, options = {}) => {
    const fontSize = options.fontSize || 9.75;
    const lineHeight = options.lineHeight || 13.5;
    doc.setFont("helvetica", options.bold ? "bold" : "normal");
    doc.setFontSize(fontSize);
    doc.setTextColor(options.muted ? 110 : 70, options.muted ? 103 : 62, options.muted ? 95 : 54);
    const lines = doc.splitTextToSize(sanitizePlainText(text), contentWidth);
    ensureSpace(lines.length * lineHeight + 4);
    doc.text(lines, margin, cursorY);
    cursorY += lines.length * lineHeight + 4;
  };

  const addBulletList = (items, options = {}) => {
    const values = (items || []).map((item) => sanitizePlainText(item)).filter(Boolean);
    if (values.length === 0) {
      return;
    }

    const fontSize = options.fontSize || 9.5;
    const lineHeight = options.lineHeight || 12.5;
    doc.setFont("helvetica", "normal");
    doc.setFontSize(fontSize);
    doc.setTextColor(70, 62, 54);

    values.forEach((item) => {
      const bulletX = margin + 4;
      const textX = margin + 12;
      const lines = doc.splitTextToSize(item, contentWidth - 16);
      ensureSpace(lines.length * lineHeight + 3);
      doc.text("-", bulletX, cursorY);
      doc.text(lines, textX, cursorY);
      cursorY += lines.length * lineHeight + 3;
    });

    cursorY += 1;
  };

  const addDefinitionList = (rows, options = {}) => {
    const filteredRows = (rows || []).filter(([, value]) => value !== null && value !== undefined && value !== "");
    if (filteredRows.length === 0) {
      return;
    }

    const fontSize = options.fontSize || 9.5;
    const lineHeight = options.lineHeight || 12.5;
    const labelWidth = options.labelWidth || 112;
    const valueX = margin + labelWidth;
    const valueWidth = contentWidth - labelWidth;

    filteredRows.forEach(([label, value]) => {
      const labelText = sanitizePlainText(label);
      const valueText = sanitizePlainText(value);

      doc.setFont("helvetica", "bold");
      doc.setFontSize(fontSize);
      const labelLines = doc.splitTextToSize(labelText, labelWidth - 10);

      doc.setFont("helvetica", "normal");
      doc.setFontSize(fontSize);
      const valueLines = doc.splitTextToSize(valueText, valueWidth);

      const rowLineCount = Math.max(labelLines.length, valueLines.length);
      const rowHeight = rowLineCount * lineHeight + 1;
      ensureSpace(rowHeight + 1);

      doc.setFont("helvetica", "bold");
      doc.setFontSize(fontSize);
      doc.setTextColor(97, 88, 79);
      doc.text(labelLines, margin, cursorY);

      doc.setFont("helvetica", "normal");
      doc.setTextColor(70, 62, 54);
      doc.text(valueLines, valueX, cursorY);

      cursorY += rowHeight;
    });

    cursorY += 1;
  };

  const addSection = (kicker, heading, body) => {
    if (cursorY > margin + 8) {
      drawRule();
    }
    addKicker(kicker);
    addHeading(heading, 16);
    body();
  };

  const reports = payload?.reports || [];
  const brandWebsite = getBrandWebsite(payload);

  addKicker("TinyDetective");
  addHeading("Counterfeit Research Evidence Dossier", 20);
  addParagraph(
    "Prepared from the captured TinyDetective investigation outputs for internal review, marketplace complaint preparation, and counsel handoff. This report is an evidence summary, not legal advice.",
    { fontSize: 10.5, lineHeight: 14.5 }
  );
  addDefinitionList([
    ["Investigation ID", payload?.investigation_id || "Unavailable"],
    ["Status", payload?.status || "Unavailable"],
    ["Created", formatReportDate(payload?.created_at)],
    ["Updated", formatReportDate(payload?.updated_at)],
    ["Brand website", brandWebsite],
  ], { labelWidth: 106 });

  reports.forEach((report, index) => {
    const sourceProduct = report.extracted_source_product || {};
    const sourceProductFeatures = (sourceProduct.features || []).filter(Boolean);
    const candidateTasks = getCandidateTasks(report);
    const discoveredCandidates = collectDiscoveredCandidates(report);
    const completedComparisons = collectCompletedComparisons(report);
    const rankedListings = getRankingSnapshots(report);
    const suggestedActions = getSuggestedActionsForReport(report);
    const suspiciousUrls = rankedListings.map((item) => String(item.product_url));
    const operationalTrace = (report.raw_agent_outputs || []).map((task) => {
      const details = [
        task.agent_name || "agent",
        task.status || "unknown",
        ...buildOperationalTrace(task),
      ];
      return details.join(" | ");
    });

    addSection(`Source ${index + 1}`, "Investigation Scope", () => {
      addDefinitionList([
        ["Input URL", report.source_url || lastSubmittedSourceUrl],
        ["Brand website", brandWebsite],
        ["Report summary", report.summary || "No summary returned."],
        ["Report error", report.error || ""],
        ["Official-store exclusions", report.excluded_official_store_count ?? 0],
      ], { labelWidth: 114 });
    });

    addSection(`Source ${index + 1}`, "Official Product Reference", () => {
      addDefinitionList([
        ["Brand", sourceProduct.brand || "Unavailable"],
        ["Product name", sourceProduct.product_name || "Unavailable"],
        ["Category", sourceProduct.category || "Unavailable"],
        ["Subcategory", sourceProduct.subcategory || "Unavailable"],
        ["SKU", sourceProduct.sku || "Unavailable"],
        ["Model", sourceProduct.model || "Unavailable"],
        ["Price", sourceProduct.price !== null && sourceProduct.price !== undefined
          ? formatCompactCurrency(sourceProduct.price, sourceProduct.currency)
          : "Unavailable"],
        ["Color", sourceProduct.color || "Unavailable"],
        ["Size", sourceProduct.size || "Unavailable"],
        ["Material", sourceProduct.material || "Unavailable"],
      ]);

      if (sourceProductFeatures.length > 0) {
        addParagraph("Key features", {
          bold: true,
          fontSize: 9.5,
          lineHeight: 12.5,
        });
        addBulletList(sourceProductFeatures, { fontSize: 9, lineHeight: 11.5 });
      }
    });

    addSection(`Source ${index + 1}`, "Ranked Listings of Concern", () => {
      if (rankedListings.length === 0) {
        addParagraph("No ranked suspicious or lookalike listings were available in this run.", {
          muted: true,
        });
        return;
      }

      rankedListings.slice(0, 5).forEach((match, rankIndex) => {
        addParagraph(
          `#${rankIndex + 1} ${match.candidate_product?.title || match.candidate_product?.model || match.product_url}`,
          { bold: true, fontSize: 10.5, lineHeight: 13.5 }
        );
        addDefinitionList([
          ["Listing URL", match.product_url],
          ["Marketplace", match.marketplace || formatHostname(match.product_url)],
          ["Seller", match.candidate_product?.seller_name || "Unavailable"],
          ["Risk score", Number(match.counterfeit_risk_score || 0).toFixed(2)],
          ["Match score", Number(match.match_score || 0).toFixed(2)],
        ], { labelWidth: 96 });
        addParagraph(`Observed rationale: ${match.reason || "No reason returned."}`, {
          fontSize: 9.25,
          lineHeight: 12.5,
        });
        addBulletList(
          getRiskReasonLines(match).map((line) => `Risk reasoning: ${line}`),
          { fontSize: 9, lineHeight: 11.5 }
        );
        addBulletList(
          getMatchReasonLines(match).map((line) => `Match reasoning: ${line}`),
          { fontSize: 9, lineHeight: 11.5 }
        );
        addBulletList(
          (match.evidence || []).slice(0, 5).map((item) => {
            const sourceValue =
              item.source_value !== null && item.source_value !== undefined ? ` | source: ${item.source_value}` : "";
            const candidateValue =
              item.candidate_value !== null && item.candidate_value !== undefined
                ? ` | candidate: ${item.candidate_value}`
                : "";
            return `Evidence - ${humanizeFieldName(item.field)}: ${item.note}${sourceValue}${candidateValue}`;
          }),
          { fontSize: 8.75, lineHeight: 11.25 }
        );
        cursorY += 2;
      });
    });

    addSection(`Source ${index + 1}`, "Suspicious URLs", () => {
      addBulletList(
        suspiciousUrls.length > 0 ? suspiciousUrls : ["No suspicious URLs were ranked in this run."]
      );
    });

    addSection(`Source ${index + 1}`, "Marketplace Search Coverage", () => {
      if (candidateTasks.length === 0) {
        addParagraph("No marketplace search tasks were recorded.", { muted: true });
        return;
      }

      addBulletList(
        candidateTasks.map((task) => {
          const query = task.output_payload?.search_query || task.input_payload?.search_query || "Unavailable";
          const site = task.output_payload?.comparison_site || task.input_payload?.comparison_site || "Unavailable";
          const candidateCount = task.output_payload?.candidate_count;
          return `${formatHostname(site)} | query: ${query} | status: ${task.status || "unknown"}${
            candidateCount !== undefined ? ` | candidates: ${candidateCount}` : ""
          }`;
        })
      );
    });

    addSection(`Source ${index + 1}`, "Discovered Listing Inventory", () => {
      if (discoveredCandidates.length === 0) {
        addParagraph("No candidate listings were captured.", { muted: true });
        return;
      }

      addBulletList(
        discoveredCandidates.map((candidate) => {
          const title = candidate.title || candidate.model || candidate.product_url;
          const price =
            candidate.price !== null && candidate.price !== undefined
              ? formatCompactCurrency(candidate.price, candidate.currency)
              : "Price unavailable";
          return `${title} | ${candidate.product_url} | marketplace: ${
            candidate.marketplace || formatHostname(candidate.product_url)
          } | seller: ${candidate.seller_name || "Unavailable"} | price: ${price} | query: ${
            candidate.discovery_query || "Unavailable"
          }`;
        })
      );
    });

    addSection(`Source ${index + 1}`, "Comparison Evidence Inventory", () => {
      if (completedComparisons.length === 0) {
        addParagraph("No completed comparison records were available.", { muted: true });
        return;
      }

      addBulletList(
        completedComparisons.map((comparison) => {
          const signals = (comparison.suspicious_signals || []).join(", ") || "None";
          return `${
            comparison.candidate_product?.title || comparison.candidate_product?.model || comparison.product_url
          } | ${comparison.product_url} | risk ${Number(comparison.counterfeit_risk_score || 0).toFixed(2)} | match ${Number(
            comparison.match_score || 0
          ).toFixed(2)} | signals: ${signals}`;
        })
      );
    });

    addSection(`Source ${index + 1}`, "Recommended Next Actions", () => {
      addBulletList(suggestedActions);
    });

    addSection(`Source ${index + 1}`, "Complaint-Prep Checklist", () => {
      addBulletList([
        "Preserve the direct listing URL for each suspicious entry and record the capture date and time on any screenshot or exported artifact.",
        "Attach trademark ownership, authorization, or registration materials separately before filing any formal complaint or legal action.",
        "Confirm seller identity, marketplace storefront details, and product identifiers before requesting takedown or asserting infringement.",
        "Separate factual observations from legal conclusions; use this dossier as supporting evidence for counsel or trust-and-safety review.",
        "If a direct link becomes unavailable, capture a screenshot of the listing or ad together with the visible seller and product details.",
      ]);
    });

    addSection(`Source ${index + 1}`, "Limitations and Gaps", () => {
      addBulletList(getReportLimitations(report));
    });

    addSection(`Source ${index + 1}`, "Operational Trace", () => {
      addBulletList(
        operationalTrace.length > 0 ? operationalTrace : ["No operational trace was captured."]
      );
    });
  });

  const pageCount = doc.getNumberOfPages();
  for (let pageNumber = 1; pageNumber <= pageCount; pageNumber += 1) {
    doc.setPage(pageNumber);
    doc.setDrawColor(225, 218, 209);
    doc.setLineWidth(0.5);
    doc.line(margin, pageHeight - margin + 2, pageWidth - margin, pageHeight - margin + 2);

    doc.setFont("helvetica", "normal");
    doc.setFontSize(8);
    doc.setTextColor(132, 124, 113);
    doc.text("TinyDetective dossier", margin, pageHeight - margin + 14);
    doc.text(`Page ${pageNumber} of ${pageCount}`, pageWidth - margin, pageHeight - margin + 14, {
      align: "right",
    });
  }

  return doc.output("blob");
}

function formatReportHtmlValue(value) {
  return escapeHtml(sanitizePlainText(value));
}

function renderReportFactGrid(rows) {
  const filteredRows = (rows || []).filter(([, value]) => value !== null && value !== undefined && value !== "");
  if (filteredRows.length === 0) {
    return "";
  }

  return `
    <dl class="pdf-report-facts">
      ${filteredRows
        .map(
          ([label, value]) => `
            <div class="pdf-report-fact">
              <dt>${formatReportHtmlValue(label)}</dt>
              <dd>${formatReportHtmlValue(value)}</dd>
            </div>
          `
        )
        .join("")}
    </dl>
  `;
}

function renderReportListBlock(items, emptyMessage = "", className = "pdf-report-list") {
  const values = (items || []).map((item) => sanitizePlainText(item)).filter(Boolean);
  if (values.length === 0) {
    return emptyMessage
      ? `<p class="pdf-report-empty">${formatReportHtmlValue(emptyMessage)}</p>`
      : "";
  }

  return `
    <ul class="${className}">
      ${values.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
    </ul>
  `;
}

function renderReportCard(title, body, options = {}) {
  const kicker = options.kicker || "Section";
  return `
    <article class="pdf-report-card ${options.className || ""}">
      <p class="pdf-report-card-kicker">${formatReportHtmlValue(kicker)}</p>
      <h3 class="pdf-report-card-title">${formatReportHtmlValue(title)}</h3>
      ${body}
    </article>
  `;
}

function buildInvestigationReportMarkup(payload) {
  const reports = payload?.reports || [];
  const brandWebsite = getBrandWebsite(payload);

  const topSummary = `
    <section class="pdf-report-shell pdf-report-shell--hero">
      <p class="pdf-report-eyebrow">TinyDetective</p>
      <h1 class="pdf-report-title">Counterfeit Research Evidence Dossier</h1>
      <p class="pdf-report-intro">
        Prepared from the captured TinyDetective investigation outputs for internal review, marketplace complaint
        preparation, and counsel handoff. This report is an evidence summary, not legal advice.
      </p>
      <div class="pdf-report-meta-grid">
        ${[
          ["Investigation ID", payload?.investigation_id || "Unavailable"],
          ["Status", payload?.status || "Unavailable"],
          ["Created", formatReportDate(payload?.created_at)],
          ["Updated", formatReportDate(payload?.updated_at)],
          ["Brand website", brandWebsite],
        ]
          .map(
            ([label, value]) => `
              <div class="pdf-report-meta-card">
                <p class="pdf-report-meta-label">${formatReportHtmlValue(label)}</p>
                <p class="pdf-report-meta-value">${formatReportHtmlValue(value)}</p>
              </div>
            `
          )
          .join("")}
      </div>
    </section>
  `;

  const sourceBlocks = reports
    .map((report, index) => {
      const sourceProduct = report.extracted_source_product || {};
      const sourceProductFeatures = (sourceProduct.features || []).filter(Boolean);
      const candidateTasks = getCandidateTasks(report);
      const discoveredCandidates = collectDiscoveredCandidates(report);
      const completedComparisons = collectCompletedComparisons(report);
      const rankedListings = getRankingSnapshots(report);
      const suggestedActions = getSuggestedActionsForReport(report);
      const suspiciousUrls = rankedListings.map((item) => String(item.product_url));
      const operationalTrace = (report.raw_agent_outputs || []).map((task) => {
        const details = [
          task.agent_name || "agent",
          task.status || "unknown",
          ...buildOperationalTrace(task),
        ];
        return details.join(" | ");
      });

      const rankedListingsMarkup =
        rankedListings.length === 0
          ? `<p class="pdf-report-empty">No ranked suspicious or lookalike listings were available in this run.</p>`
          : `
              <div class="pdf-report-match-stack">
                ${rankedListings
                  .slice(0, 5)
                  .map((match, rankIndex) => {
                    const reasoningLines = [
                      ...getRiskReasonLines(match).map((line) => `Risk reasoning: ${line}`),
                      ...getMatchReasonLines(match).map((line) => `Match reasoning: ${line}`),
                      ...(match.evidence || []).slice(0, 5).map((item) => {
                        const sourceValue =
                          item.source_value !== null && item.source_value !== undefined
                            ? ` | source: ${item.source_value}`
                            : "";
                        const candidateValue =
                          item.candidate_value !== null && item.candidate_value !== undefined
                            ? ` | candidate: ${item.candidate_value}`
                            : "";
                        return `Evidence - ${humanizeFieldName(item.field)}: ${item.note}${sourceValue}${candidateValue}`;
                      }),
                    ];

                    return `
                      <article class="pdf-report-match-card">
                        <div class="pdf-report-match-head">
                          <div>
                            <p class="pdf-report-match-index">#${rankIndex + 1}</p>
                            <h4 class="pdf-report-match-title">${formatReportHtmlValue(
                              match.candidate_product?.title || match.candidate_product?.model || match.product_url
                            )}</h4>
                          </div>
                          <div class="pdf-report-chip-row">
                            <span class="pdf-report-chip">Risk ${formatReportHtmlValue(
                              Number(match.counterfeit_risk_score || 0).toFixed(2)
                            )}</span>
                            <span class="pdf-report-chip">Match ${formatReportHtmlValue(
                              Number(match.match_score || 0).toFixed(2)
                            )}</span>
                            <span class="pdf-report-chip">${formatReportHtmlValue(
                              match.marketplace || formatHostname(match.product_url)
                            )}</span>
                          </div>
                        </div>
                        ${renderReportFactGrid([
                          ["Listing URL", match.product_url],
                          ["Seller", match.candidate_product?.seller_name || "Unavailable"],
                          ["Observed rationale", match.reason || "No reason returned."],
                        ])}
                        ${renderReportListBlock(
                          reasoningLines,
                          "No supporting reasoning was returned.",
                          "pdf-report-list pdf-report-list--tight"
                        )}
                      </article>
                    `;
                  })
                  .join("")}
              </div>
            `;

      return `
        <section class="pdf-report-shell pdf-report-shell--source">
          <div class="pdf-report-source-header">
            <p class="pdf-report-eyebrow">Source ${index + 1}</p>
            <h2 class="pdf-report-source-title">${formatReportHtmlValue(
              sourceProduct.product_name || sourceProduct.model || report.source_url || `Source ${index + 1}`
            )}</h2>
            <p class="pdf-report-source-summary">${formatReportHtmlValue(
              report.summary || "No summary returned."
            )}</p>
          </div>

          <div class="pdf-report-grid pdf-report-grid--two">
            ${renderReportCard(
              "Investigation Scope",
              renderReportFactGrid([
                ["Input URL", report.source_url || lastSubmittedSourceUrl],
                ["Brand website", brandWebsite],
                ["Report summary", report.summary || "No summary returned."],
                ["Report error", report.error || ""],
                ["Official-store exclusions", report.excluded_official_store_count ?? 0],
              ]),
              { kicker: "Case file" }
            )}
            ${renderReportCard(
              "Official Product Reference",
              [
                renderReportFactGrid([
                  ["Brand", sourceProduct.brand || "Unavailable"],
                  ["Product name", sourceProduct.product_name || "Unavailable"],
                  ["Category", sourceProduct.category || "Unavailable"],
                  ["Subcategory", sourceProduct.subcategory || "Unavailable"],
                  ["SKU", sourceProduct.sku || "Unavailable"],
                  ["Model", sourceProduct.model || "Unavailable"],
                  [
                    "Price",
                    sourceProduct.price !== null && sourceProduct.price !== undefined
                      ? formatCompactCurrency(sourceProduct.price, sourceProduct.currency)
                      : "Unavailable",
                  ],
                  ["Color", sourceProduct.color || "Unavailable"],
                  ["Size", sourceProduct.size || "Unavailable"],
                  ["Material", sourceProduct.material || "Unavailable"],
                ]),
                sourceProductFeatures.length > 0
                  ? `
                      <div class="pdf-report-subsection">
                        <p class="pdf-report-mini-label">Key features</p>
                        ${renderReportListBlock(
                          sourceProductFeatures,
                          "",
                          "pdf-report-list pdf-report-list--tight"
                        )}
                      </div>
                    `
                  : "",
              ].join(""),
              { kicker: "Official reference" }
            )}
          </div>

          ${renderReportCard("Ranked Listings of Concern", rankedListingsMarkup, {
            kicker: "Priority queue",
          })}

          <div class="pdf-report-grid pdf-report-grid--two">
            ${renderReportCard(
              "Suspicious URLs",
              renderReportListBlock(
                suspiciousUrls,
                "No suspicious URLs were ranked in this run."
              ),
              { kicker: "Link set" }
            )}
            ${renderReportCard(
              "Marketplace Search Coverage",
              candidateTasks.length === 0
                ? `<p class="pdf-report-empty">No marketplace search tasks were recorded.</p>`
                : renderReportListBlock(
                    candidateTasks.map((task) => {
                      const query =
                        task.output_payload?.search_query || task.input_payload?.search_query || "Unavailable";
                      const site =
                        task.output_payload?.comparison_site ||
                        task.input_payload?.comparison_site ||
                        "Unavailable";
                      const candidateCount = task.output_payload?.candidate_count;
                      return `${formatHostname(site)} | query: ${query} | status: ${task.status || "unknown"}${
                        candidateCount !== undefined ? ` | candidates: ${candidateCount}` : ""
                      }`;
                    })
                  ),
              { kicker: "Search log" }
            )}
          </div>

          ${renderReportCard(
            "Discovered Listing Inventory",
            discoveredCandidates.length === 0
              ? `<p class="pdf-report-empty">No candidate listings were captured.</p>`
              : renderReportListBlock(
                  discoveredCandidates.map((candidate) => {
                    const title = candidate.title || candidate.model || candidate.product_url;
                    const price =
                      candidate.price !== null && candidate.price !== undefined
                        ? formatCompactCurrency(candidate.price, candidate.currency)
                        : "Price unavailable";
                    return `${title} | ${candidate.product_url} | marketplace: ${
                      candidate.marketplace || formatHostname(candidate.product_url)
                    } | seller: ${candidate.seller_name || "Unavailable"} | price: ${price} | query: ${
                      candidate.discovery_query || "Unavailable"
                    }`;
                  }),
                  "",
                  "pdf-report-list pdf-report-list--compact"
                ),
            { kicker: "Inventory" }
          )}

          ${renderReportCard(
            "Comparison Evidence Inventory",
            completedComparisons.length === 0
              ? `<p class="pdf-report-empty">No completed comparison records were available.</p>`
              : renderReportListBlock(
                  completedComparisons.map((comparison) => {
                    const signals = (comparison.suspicious_signals || []).join(", ") || "None";
                    return `${
                      comparison.candidate_product?.title ||
                      comparison.candidate_product?.model ||
                      comparison.product_url
                    } | ${comparison.product_url} | risk ${Number(
                      comparison.counterfeit_risk_score || 0
                    ).toFixed(2)} | match ${Number(comparison.match_score || 0).toFixed(2)} | signals: ${signals}`;
                  }),
                  "",
                  "pdf-report-list pdf-report-list--compact"
                ),
            { kicker: "Evidence ledger" }
          )}

          <div class="pdf-report-grid pdf-report-grid--two">
            ${renderReportCard(
              "Recommended Next Actions",
              renderReportListBlock(suggestedActions, ""),
              { kicker: "Action queue" }
            )}
            ${renderReportCard(
              "Limitations and Gaps",
              renderReportListBlock(getReportLimitations(report), ""),
              { kicker: "Guardrails" }
            )}
          </div>

          ${renderReportCard(
            "Complaint-Prep Checklist",
            renderReportListBlock([
              "Preserve the direct listing URL for each suspicious entry and record the capture date and time on any screenshot or exported artifact.",
              "Attach trademark ownership, authorization, or registration materials separately before filing any formal complaint or legal action.",
              "Confirm seller identity, marketplace storefront details, and product identifiers before requesting takedown or asserting infringement.",
              "Separate factual observations from legal conclusions; use this dossier as supporting evidence for counsel or trust-and-safety review.",
              "If a direct link becomes unavailable, capture a screenshot of the listing or ad together with the visible seller and product details.",
            ]),
            { kicker: "Preparation" }
          )}

          ${renderReportCard(
            "Operational Trace",
            renderReportListBlock(
              operationalTrace.length > 0 ? operationalTrace : ["No operational trace was captured."],
              "",
              "pdf-report-list pdf-report-list--compact"
            ),
            { kicker: "Runtime trace" }
          )}

          <div class="pdf-report-footer">
            <span>TinyDetective research dossier</span>
            <span>Updated ${formatReportHtmlValue(formatReportDate(payload?.updated_at))}</span>
          </div>
        </section>
      `;
    })
    .join("");

  return `
    <div class="pdf-report-root">
      ${topSummary}
      ${sourceBlocks}
    </div>
  `;
}

function createInvestigationReportElement(payload) {
  const container = document.createElement("div");
  container.className = "pdf-report-mount";
  container.setAttribute("aria-hidden", "true");
  container.style.position = "fixed";
  container.style.left = "-100000px";
  container.style.top = "0";
  container.style.width = "820px";
  container.style.pointerEvents = "none";
  container.style.zIndex = "-1";

  container.innerHTML = `
    <style>
      .pdf-report-root {
        width: 760px;
        padding: 24px;
        color: #433a32;
        font-family: "Manrope", sans-serif;
        background:
          radial-gradient(circle at 12% 14%, rgba(239, 228, 208, 0.78), transparent 26%),
          radial-gradient(circle at 86% 4%, rgba(214, 229, 228, 0.44), transparent 30%),
          linear-gradient(180deg, #fbf8f2 0%, #f2eadf 100%);
      }

      .pdf-report-root,
      .pdf-report-root *,
      .pdf-report-root *::before,
      .pdf-report-root *::after {
        box-sizing: border-box;
      }

      .pdf-report-shell {
        break-inside: avoid;
        page-break-inside: avoid;
        padding: 28px;
        border: 1px solid #d8cfc1;
        border-radius: 28px;
        background: rgba(255, 255, 255, 0.86);
        box-shadow:
          0 22px 60px rgba(75, 60, 42, 0.08),
          inset 0 1px 0 rgba(255, 255, 255, 0.78);
      }

      .pdf-report-shell + .pdf-report-shell {
        margin-top: 18px;
      }

      .pdf-report-shell--source {
        padding-top: 24px;
      }

      .pdf-report-eyebrow,
      .pdf-report-card-kicker,
      .pdf-report-mini-label,
      .pdf-report-meta-label,
      .pdf-report-match-index {
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        font-size: 10px;
        font-weight: 800;
        color: #928577;
      }

      .pdf-report-title,
      .pdf-report-source-title {
        margin: 0;
        font-family: "Instrument Serif", serif;
        font-weight: 400;
        letter-spacing: -0.02em;
        color: #352c26;
      }

      .pdf-report-title {
        font-size: 42px;
        line-height: 0.95;
      }

      .pdf-report-source-title {
        font-size: 28px;
        line-height: 0.98;
      }

      .pdf-report-intro,
      .pdf-report-source-summary,
      .pdf-report-empty,
      .pdf-report-note,
      .pdf-report-meta-value,
      .pdf-report-fact dd,
      .pdf-report-list {
        color: #5a5048;
      }

      .pdf-report-intro,
      .pdf-report-source-summary {
        margin: 14px 0 0;
        font-size: 14px;
        line-height: 1.55;
      }

      .pdf-report-meta-grid,
      .pdf-report-grid {
        display: grid;
        gap: 16px;
        margin-top: 22px;
      }

      .pdf-report-meta-grid,
      .pdf-report-grid--two {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .pdf-report-meta-card,
      .pdf-report-card,
      .pdf-report-match-card {
        break-inside: avoid;
        page-break-inside: avoid;
        border: 1px solid #e1d8cc;
        border-radius: 20px;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.94) 0%, rgba(248, 243, 235, 0.92) 100%);
      }

      .pdf-report-meta-card {
        padding: 14px 16px;
      }

      .pdf-report-meta-value {
        margin: 8px 0 0;
        font-size: 13px;
        line-height: 1.5;
        word-break: break-word;
      }

      .pdf-report-card {
        padding: 18px;
      }

      .pdf-report-card-title {
        margin: 8px 0 0;
        font-size: 18px;
        font-weight: 800;
        line-height: 1.18;
        color: #352c26;
      }

      .pdf-report-facts {
        display: grid;
        gap: 10px;
        margin: 14px 0 0;
      }

      .pdf-report-fact {
        display: grid;
        grid-template-columns: 116px minmax(0, 1fr);
        gap: 12px;
        align-items: start;
      }

      .pdf-report-fact dt {
        margin: 0;
        font-size: 12px;
        font-weight: 800;
        line-height: 1.4;
        color: #827568;
      }

      .pdf-report-fact dd {
        margin: 0;
        font-size: 12px;
        line-height: 1.55;
        word-break: break-word;
      }

      .pdf-report-list {
        margin: 14px 0 0;
        padding-left: 18px;
        display: grid;
        gap: 8px;
        font-size: 12px;
        line-height: 1.5;
      }

      .pdf-report-list--tight {
        gap: 6px;
        font-size: 11.5px;
      }

      .pdf-report-list--compact {
        gap: 5px;
        font-size: 11px;
        line-height: 1.45;
      }

      .pdf-report-match-stack {
        display: grid;
        gap: 12px;
        margin-top: 14px;
      }

      .pdf-report-match-card {
        padding: 14px;
      }

      .pdf-report-match-head {
        display: grid;
        gap: 10px;
      }

      .pdf-report-match-title {
        margin: 8px 0 0;
        font-size: 14px;
        line-height: 1.35;
        color: #352c26;
      }

      .pdf-report-chip-row {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }

      .pdf-report-chip {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        background: #efe6d8;
        color: #5d4d40;
        font-size: 11px;
        font-weight: 800;
      }

      .pdf-report-subsection {
        margin-top: 14px;
        padding-top: 14px;
        border-top: 1px solid #e6ded2;
      }

      .pdf-report-footer {
        margin-top: 16px;
        display: flex;
        justify-content: space-between;
        gap: 16px;
        font-size: 10px;
        text-transform: uppercase;
        letter-spacing: 0.16em;
        color: #928577;
      }
    </style>
    ${buildInvestigationReportMarkup(payload)}
  `;

  return container;
}

async function buildInvestigationPdf(payload) {
  return buildInvestigationPdfFallback(payload);
}

function getInvestigationReportDocumentStyles() {
  return `
    :root {
      color-scheme: light;
    }

    * {
      box-sizing: border-box;
    }

    html,
    body {
      margin: 0;
      padding: 0;
      min-height: 100%;
    }

    body {
      font-family: "Manrope", sans-serif;
      color: #433a32;
      line-height: 1.5;
      background:
        radial-gradient(circle at 12% 14%, rgba(239, 228, 208, 0.78), transparent 26%),
        radial-gradient(circle at 86% 4%, rgba(214, 229, 228, 0.44), transparent 30%),
        linear-gradient(180deg, #fbf8f2 0%, #f2eadf 100%);
    }

    .pdf-report-page {
      width: min(1180px, calc(100% - 48px));
      margin: 0 auto;
      padding: 32px 0 48px;
    }

    .pdf-report-root {
      display: grid;
      gap: 18px;
    }

    .pdf-report-shell {
      break-inside: avoid;
      padding: 28px;
      border: 1px solid #d8cfc1;
      border-radius: 28px;
      background: rgba(255, 255, 255, 0.86);
      box-shadow:
        0 22px 60px rgba(75, 60, 42, 0.08),
        inset 0 1px 0 rgba(255, 255, 255, 0.78);
    }

    .pdf-report-eyebrow,
    .pdf-report-card-kicker,
    .pdf-report-mini-label,
    .pdf-report-meta-label,
    .pdf-report-match-index {
      margin: 0;
      text-transform: uppercase;
      letter-spacing: 0.18em;
      font-size: 10px;
      font-weight: 800;
      color: #928577;
    }

    .pdf-report-title,
    .pdf-report-source-title {
      margin: 0;
      font-family: "Instrument Serif", serif;
      font-weight: 400;
      letter-spacing: -0.02em;
      color: #352c26;
      line-height: 0.96;
    }

    .pdf-report-title {
      font-size: clamp(2.8rem, 5vw, 4.35rem);
    }

    .pdf-report-source-title {
      font-size: clamp(2rem, 3vw, 2.6rem);
    }

    .pdf-report-intro,
    .pdf-report-source-summary {
      margin: 14px 0 0;
      font-size: 1.02rem;
      line-height: 1.55;
      color: #5a5048;
    }

    .pdf-report-meta-grid,
    .pdf-report-grid {
      display: grid;
      gap: 16px;
      margin-top: 22px;
    }

    .pdf-report-meta-grid,
    .pdf-report-grid--two {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .pdf-report-meta-card,
    .pdf-report-card,
    .pdf-report-match-card {
      border: 1px solid #e1d8cc;
      border-radius: 20px;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.94) 0%, rgba(248, 243, 235, 0.92) 100%);
    }

    .pdf-report-meta-card {
      padding: 14px 16px;
    }

    .pdf-report-meta-value {
      margin: 8px 0 0;
      font-size: 0.95rem;
      line-height: 1.5;
      color: #5a5048;
      word-break: break-word;
    }

    .pdf-report-card {
      padding: 18px;
    }

    .pdf-report-card-title {
      margin: 8px 0 0;
      font-size: 1.2rem;
      font-weight: 800;
      line-height: 1.18;
      color: #352c26;
    }

    .pdf-report-facts {
      display: grid;
      gap: 10px;
      margin: 14px 0 0;
    }

    .pdf-report-fact {
      display: grid;
      grid-template-columns: 116px minmax(0, 1fr);
      gap: 12px;
      align-items: start;
    }

    .pdf-report-fact dt {
      margin: 0;
      font-size: 0.83rem;
      font-weight: 800;
      line-height: 1.4;
      color: #827568;
    }

    .pdf-report-fact dd {
      margin: 0;
      font-size: 0.9rem;
      line-height: 1.55;
      color: #5a5048;
      word-break: break-word;
    }

    .pdf-report-list {
      margin: 14px 0 0;
      padding-left: 18px;
      display: grid;
      gap: 8px;
      font-size: 0.92rem;
      line-height: 1.5;
      color: #5a5048;
    }

    .pdf-report-list--tight {
      gap: 6px;
      font-size: 0.88rem;
    }

    .pdf-report-list--compact {
      gap: 5px;
      font-size: 0.85rem;
      line-height: 1.45;
    }

    .pdf-report-empty {
      margin: 14px 0 0;
      color: #5a5048;
    }

    .pdf-report-match-stack {
      display: grid;
      gap: 12px;
      margin-top: 14px;
    }

    .pdf-report-match-card {
      padding: 14px;
    }

    .pdf-report-match-head {
      display: grid;
      gap: 10px;
    }

    .pdf-report-match-title {
      margin: 8px 0 0;
      font-size: 1rem;
      line-height: 1.35;
      color: #352c26;
    }

    .pdf-report-chip-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .pdf-report-chip {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      background: #efe6d8;
      color: #5d4d40;
      font-size: 0.78rem;
      font-weight: 800;
    }

    .pdf-report-subsection {
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid #e6ded2;
    }

    .pdf-report-footer {
      margin-top: 16px;
      display: flex;
      justify-content: space-between;
      gap: 16px;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      color: #928577;
    }

    .pdf-report-toolbar {
      position: sticky;
      top: 0;
      z-index: 2;
      display: flex;
      justify-content: flex-end;
      padding: 16px 24px 0;
      background: linear-gradient(180deg, rgba(251, 248, 242, 0.95), rgba(251, 248, 242, 0));
      backdrop-filter: blur(8px);
    }

    .pdf-report-print-button {
      border: 0;
      border-radius: 999px;
      padding: 10px 16px;
      background: #433a32;
      color: #f8f1e7;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }

    @media (max-width: 860px) {
      .pdf-report-page {
        width: min(100% - 24px, 100%);
        padding-top: 20px;
      }

      .pdf-report-meta-grid,
      .pdf-report-grid--two {
        grid-template-columns: 1fr;
      }

      .pdf-report-fact {
        grid-template-columns: 1fr;
        gap: 4px;
      }

      .pdf-report-shell {
        padding: 20px;
      }

      .pdf-report-footer {
        flex-direction: column;
      }
    }

    @media print {
      body {
        background: #ffffff;
      }

      .pdf-report-toolbar {
        display: none;
      }

      .pdf-report-page {
        width: 100%;
        padding: 0;
      }

      .pdf-report-shell,
      .pdf-report-card,
      .pdf-report-match-card,
      .pdf-report-meta-card {
        box-shadow: none;
      }

      @page {
        size: letter;
        margin: 0.55in;
      }
    }
  `;
}

function buildInvestigationReportDocument(payload) {
  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>TinyDetective Evidence Dossier</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link
      href="https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Manrope:wght@400;500;600;700;800&display=swap"
      rel="stylesheet"
    />
    <style>${getInvestigationReportDocumentStyles()}</style>
  </head>
  <body>
    <div class="pdf-report-toolbar">
      <button class="pdf-report-print-button" type="button" onclick="window.print()">Print / Save PDF</button>
    </div>
    <main class="pdf-report-page">
      ${buildInvestigationReportMarkup(payload)}
    </main>
  </body>
</html>`;
}

function revokeCurrentReportDocumentUrl() {
  if (!currentReportDocumentUrl) {
    return;
  }
  window.URL.revokeObjectURL(currentReportDocumentUrl);
  currentReportDocumentUrl = null;
}

function resetReportScene() {
  revokeCurrentReportDocumentUrl();
  if (reportPdfFrame) {
    reportPdfFrame.removeAttribute("srcdoc");
    reportPdfFrame.removeAttribute("src");
  }
  if (reportOpenButton) {
    reportOpenButton.hidden = true;
    reportOpenButton.textContent = "Open in new tab";
  }
  if (reportMeta) {
    setTextContent(reportMeta, "The styled report will be prepared from the captured investigation data.");
  }
  if (reportNote) {
    setTextContent(reportNote, "Generate a report from step 5 to review it here.");
  }
}

function presentStyledReport(payload) {
  const reportDocument = buildInvestigationReportDocument(payload);
  const objectUrl = window.URL.createObjectURL(new Blob([reportDocument], { type: "text/html" }));
  revokeCurrentReportDocumentUrl();
  currentReportDocumentUrl = objectUrl;

  if (reportPdfFrame) {
    reportPdfFrame.srcdoc = reportDocument;
    reportPdfFrame.src = objectUrl;
  }
  if (reportOpenButton) {
    reportOpenButton.hidden = false;
    reportOpenButton.textContent = "Open / print";
  }
  if (reportNote) {
    setTextContent(reportNote, "The styled evidence dossier is ready for review.");
  }
  if (reportMeta) {
    setTextContent(
      reportMeta,
      `Investigation ${payload?.investigation_id || "Unavailable"} | Updated ${formatReportDate(
        payload?.updated_at
      )} | Brand website ${getBrandWebsite(payload)}`
    );
  }
  setPhase("report");
}

function updateGenerateReportButton(payload) {
  if (!generateReportButton) {
    return;
  }

  const reports = payload?.reports || [];
  const canGenerate = reports.some(
    (report) =>
      Boolean(report.source_url) ||
      getRankingSnapshots(report).length > 0 ||
      collectDiscoveredCandidates(report).length > 0 ||
      (report.raw_agent_outputs || []).length > 0
  );

  generateReportButton.disabled = !canGenerate || reportGenerationInFlight;
  generateReportButton.textContent = reportGenerationInFlight
    ? "Generating report..."
    : defaultGenerateReportButtonLabel;
}

function hasStarted(status) {
  return !["pending"].includes(String(status || "pending").toLowerCase());
}

function combineStates(states) {
  const normalizedStates = states.map((status) => String(status || "pending").toLowerCase());
  if (normalizedStates.some((status) => status === "failed")) {
    return "failed";
  }
  if (normalizedStates.some((status) => status === "delayed")) {
    return "delayed";
  }
  if (normalizedStates.some((status) => status === "running")) {
    return "running";
  }
  if (normalizedStates.every((status) => status === "completed")) {
    return "completed";
  }
  if (normalizedStates.some((status) => status === "queued")) {
    return "queued";
  }
  return "pending";
}

function deriveTimelineStates(stepStates) {
  const candidateState = stepStates.candidate_discovery || "pending";
  const rankingStarted = hasStarted(stepStates.ranking) || hasStarted(stepStates.research_summary);

  return {
    source: stepStates.source_extraction || "pending",
    search: candidateState,
    candidates: rankingStarted || hasStarted(stepStates.product_comparison)
      ? "completed"
      : candidateState,
    analysis: rankingStarted
      ? "completed"
      : combineStates([stepStates.product_comparison, stepStates.evidence]),
    ranking: stepStates.ranking || "pending",
  };
}

function getFocusedTimelineStage(timelineStates) {
  const orderedStages = timelineStageDefinitions.map((stage) => [
    stage.key,
    timelineStates[stage.key] || "pending",
  ]);

  for (const status of ["failed", "delayed", "running", "queued"]) {
    const activeStage = orderedStages.find(([, stageStatus]) => stageStatus === status);
    if (activeStage) {
      return activeStage[0];
    }
  }

  const firstPendingIndex = orderedStages.findIndex(([, stageStatus]) => stageStatus === "pending");
  if (firstPendingIndex === 0) {
    return orderedStages[0][0];
  }
  if (firstPendingIndex > 0) {
    return orderedStages[firstPendingIndex - 1][0];
  }

  return orderedStages[orderedStages.length - 1][0];
}

function setFocusedTimelineStage(stageKey, options = {}) {
  const stageIndex = timelineStageDefinitions.findIndex((stage) => stage.key === stageKey);
  if (stageIndex === -1) {
    return;
  }

  const shouldJump = options.immediate || !timelineTrack || !timelineTrack.dataset.ready;
  const stageChanged = activeTimelineStage !== stageKey;
  activeTimelineStage = stageKey;

  if (timelineTrack) {
    if (shouldJump) {
      const previousTransition = timelineTrack.style.transition;
      timelineTrack.style.transition = "none";
      timelineTrack.style.transform = `translateY(-${stageIndex * 100}%)`;
      void timelineTrack.offsetHeight;
      timelineTrack.style.transition = previousTransition;
    } else if (stageChanged) {
      timelineTrack.style.transform = `translateY(-${stageIndex * 100}%)`;
    }
    timelineTrack.dataset.ready = "true";
  }

  timelineStageDefinitions.forEach((stage) => {
    const isActive = stage.key === stageKey;
    timelineStageItems[stage.key]?.classList.toggle("is-active", isActive);
    timelineRailItems[stage.key]?.classList.toggle("is-active", isActive);
  });
}

function getSourcePreviewUrl(report) {
  return report?.source_url || lastSubmittedSourceUrl || parseLines(sourceUrlsInput.value)[0] || "";
}

function getCandidateTasks(report) {
  return (report?.raw_agent_outputs || []).filter((task) => task.agent_name === "candidate_discovery");
}

function getComparisonTasks(report) {
  return (report?.raw_agent_outputs || []).filter((task) => task.agent_name === "product_comparison");
}

function getEvidenceTasks(report) {
  return (report?.raw_agent_outputs || []).filter((task) => task.agent_name === "evidence");
}

function getRankingTask(report) {
  return (report?.raw_agent_outputs || []).find((task) => task.agent_name === "ranking") || null;
}

function collectDiscoveredCandidates(report) {
  const candidatesByUrl = new Map();

  getCandidateTasks(report).forEach((task) => {
    (task.output_payload?.candidates || []).forEach((candidate) => {
      if (!candidatesByUrl.has(candidate.product_url)) {
        candidatesByUrl.set(candidate.product_url, {
          ...candidate,
          discovery_query: task.output_payload?.search_query || task.input_payload?.search_query || "",
          comparison_site:
            task.output_payload?.comparison_site || task.input_payload?.comparison_site || "",
        });
      }
    });
  });

  return [...candidatesByUrl.values()];
}

function collectCompletedComparisons(report) {
  const evidenceByUrl = new Map();
  getEvidenceTasks(report).forEach((task) => {
    const productUrl = task.input_payload?.product_url;
    if (productUrl && task.output_payload?.evidence) {
      evidenceByUrl.set(productUrl, task.output_payload.evidence);
    }
  });

  return getComparisonTasks(report)
    .filter((task) => task.output_payload?.comparison)
    .map((task) => {
      const comparison = { ...task.output_payload.comparison };
      if ((!comparison.evidence || comparison.evidence.length === 0) && evidenceByUrl.has(comparison.product_url)) {
        comparison.evidence = evidenceByUrl.get(comparison.product_url);
      }
      return comparison;
    });
}

function getRankingSnapshots(report) {
  if (report?.top_matches?.length) {
    return sortMatchesByCounterfeitRisk(report.top_matches);
  }
  return sortMatchesByCounterfeitRisk(collectCompletedComparisons(report));
}

function parseLines(value) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function setPhase(phase) {
  currentPhase = phase;
  body.dataset.phase = phase;
}

function setHistoryMenuOpen(isOpen) {
  if (!historyButton || !historyDropdown) {
    return;
  }

  historyButton.setAttribute("aria-expanded", String(isOpen));
  historyDropdown.hidden = !isOpen;
}

function setComposerInvalid(isInvalid) {
  promptComposer.classList.toggle("is-invalid", isInvalid);
}

function syncPromptHeight() {
  sourceUrlsInput.style.height = "0px";
  sourceUrlsInput.style.height = `${Math.min(sourceUrlsInput.scrollHeight, 240)}px`;
}

function setStatus(status) {
  const normalizedStatus = String(status || "idle").toLowerCase();
  statusPill.dataset.status = normalizedStatus;
  statusPill.textContent = statusLabels[normalizedStatus] || status;
}

function setSubmitting(isSubmitting) {
  runButton.disabled = isSubmitting;
  runButton.setAttribute("aria-busy", String(isSubmitting));
  runButton.textContent = isSubmitting ? "Investigating..." : defaultRunButtonLabel;
}

function startNewInvestigation() {
  if (pollTimer) {
    window.clearTimeout(pollTimer);
    pollTimer = null;
  }

  clearPersistedInvestigationId();
  latestInvestigationPayload = null;
  activeTimelineStage = "source";
  resetProgressTracking();
  setStatus("idle");
  updateProgressUI({
    overview: "No investigation running yet.",
    detail: "Start a new investigation from the prompt.",
    percent: 0,
    stepStates: Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "pending"])),
  });
  renderTimeline(null);
  renderEmptyState("Add official product page URLs to compare them against live marketplace listings.");
  setSubmitting(false);
  setComposerInvalid(false);
  resetReportScene();
  setHistoryMenuOpen(false);
  sourceUrlsInput.value = "";
  syncPromptHeight();
  setPhase("prompt");
  sourceUrlsInput.focus();
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
  resetReportScene();
  setPhase("progress");
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

function formatRunSource(run) {
  const sourceUrl = run?.primary_source_url;
  const sourceTitle = sanitizePlainText(run?.primary_source_title || "");

  if (!sourceUrl) {
    return {
      title: sourceTitle || "Investigation",
      detail: "No source URL saved",
      full: "",
    };
  }

  try {
    const url = new URL(sourceUrl);
    const pathname = decodeURIComponent(url.pathname || "/").replace(/\/$/, "") || "/";
    return {
      title: sourceTitle || url.hostname.replace(/^www\./, ""),
      detail: pathname === "/" ? "Homepage" : pathname,
      full: url.toString(),
    };
  } catch {
    return {
      title: sourceTitle || sourceUrl,
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
    const source = formatRunSource(run);

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
    primary_source_url: payload.reports?.[0]?.source_url || existingRun?.primary_source_url || null,
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
  } catch {
    if (pastRunsCache.length === 0) {
      pastRunsNode.innerHTML =
        '<p class="empty-state">Saved investigations could not be loaded right now.</p>';
    }
  }
}

function renderEmptyState(message) {
  if (!resultsNode) {
    return;
  }
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

function updateProgressUI({ overview, detail, percent, stepStates, timelineStates, focusedStage }) {
  progressOverview.textContent = overview;
  progressText.textContent = detail;
  progressFill.style.width = `${percent}%`;
  progressTrack.setAttribute("aria-valuenow", String(percent));

  const nextTimelineStates = timelineStates || deriveTimelineStates(stepStates);
  timelineStageDefinitions.forEach((stage) => {
    const node = timelineStageItems[stage.key];
    const railNode = timelineRailItems[stage.key];
    const status = nextTimelineStates[stage.key] || "pending";
    node.dataset.status = status;
    node.querySelector(".timeline-step-state").textContent = timelineStateLabels[status] || status;
    if (railNode) {
      railNode.dataset.status = status;
    }
  });

  setFocusedTimelineStage(focusedStage || getFocusedTimelineStage(nextTimelineStates));
}

function resetProgressTracking() {
  const stepStates = Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "pending"]));
  updateProgressUI({
    overview: "No investigation running yet.",
    detail: "Waiting for an investigation to start.",
    percent: 0,
    stepStates,
  });
  setFocusedTimelineStage("source", { immediate: true });
  renderTimeline(null);
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
  const timelineStates = deriveTimelineStates(activeStepStates);
  const focusedTimelineStage = getFocusedTimelineStage(timelineStates);
  const focusedTimelineLabel =
    timelineStageDefinitions.find((stage) => stage.key === focusedTimelineStage)?.label ||
    "Investigation";
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
    overview = totalSources > 1 ? `Source 1 of ${totalSources} · ${focusedTimelineLabel}` : focusedTimelineLabel;
    detail = activeReport?.summary || "Preparing the investigation context.";
  } else if (payload.status === "running") {
    overview =
      totalSources > 1
        ? `Source ${sourcePosition} of ${totalSources} · ${focusedTimelineLabel}`
        : focusedTimelineLabel;
    detail = activeReport?.summary || "Investigation is in progress.";
  } else if (payload.status === "delayed") {
    overview =
      totalSources > 1
        ? `Source ${sourcePosition} of ${totalSources} · ${focusedTimelineLabel}`
        : focusedTimelineLabel;
    detail = activeReport?.summary || "TinyFish is still working on the active step.";
  } else if (payload.status === "completed") {
    overview =
      totalSources > 1
        ? `Completed ${completedReports} of ${totalSources} · ${focusedTimelineLabel}`
        : focusedTimelineLabel;
    detail = activeReport?.summary || "The investigation finished successfully.";
  } else if (payload.status === "failed") {
    overview = focusedTimelineLabel;
    detail = payload.error || activeReport?.error || "The investigation ended with an error.";
  }

  if (activeStep?.label && payload.status !== "completed") {
    detail = `${activeStep.label}. ${detail}`;
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
    timelineStates,
    focusedStage: focusedTimelineStage,
  });

  renderTimeline(activeReport);
}

function renderSourceStage(report) {
  const sourceUrl = getSourcePreviewUrl(report);
  const product = report?.extracted_source_product || null;

  setTextContent(timelineSourceUrl, sourceUrl || "No source URL selected yet.");
  timelineSourceLink.hidden = !sourceUrl;
  if (sourceUrl) {
    timelineSourceLink.href = sourceUrl;
    if (timelineSourceFrame.dataset.sourceUrl !== sourceUrl) {
      timelineSourceFrame.src = sourceUrl;
      timelineSourceFrame.dataset.sourceUrl = sourceUrl;
    }
  } else if (timelineSourceFrame.dataset.sourceUrl) {
    timelineSourceFrame.removeAttribute("src");
    timelineSourceFrame.dataset.sourceUrl = "";
  }

  if (!sourceUrl) {
    setTextContent(timelineNotes.source, "Waiting for the official product page.");
    setInnerHtml(
      timelineSourceMeta,
      '<p class="empty-state">The source page will appear here after you start an investigation.</p>'
    );
    return;
  }

  const metaHtml = product
    ? `
      <span class="meta-label">Extracted profile</span>
      <div class="meta-title">${escapeHtml(
        `${product.brand || "Unknown brand"} · ${product.product_name || "Unknown product"}`
      )}</div>
      <div class="meta-grid">
        <div><strong>Category</strong>${escapeHtml(product.category || "Unavailable")}</div>
        <div><strong>SKU</strong>${escapeHtml(product.sku || "Unavailable")}</div>
        <div><strong>Model</strong>${escapeHtml(product.model || "Unavailable")}</div>
        <div><strong>Price</strong>${escapeHtml(
          product.price !== null && product.price !== undefined
            ? formatCompactCurrency(product.price, product.currency)
            : "Unavailable"
        )}</div>
      </div>
    `
    : `
      <span class="meta-label">Source status</span>
      <div class="meta-title">${escapeHtml(formatHostname(sourceUrl))}</div>
      <p class="empty-state">Extracted source attributes will populate here once the source step finishes.</p>
    `;

  setTextContent(
    timelineNotes.source,
    product
      ? "Official product details extracted from the source page."
      : "Showing the live source page while extraction is still running."
  );
  setInnerHtml(timelineSourceMeta, metaHtml);
}

function renderSearchStage(report) {
  const searchTasks = getCandidateTasks(report);
  const visibleSearchTasks = searchTasks.slice(0, 3);

  if (searchTasks.length === 0) {
    setTextContent(
      timelineNotes.search,
      "Search queries will appear here as TinyFish fans out across marketplaces."
    );
    setInnerHtml(
      timelineSearchLog,
      '<p class="empty-state">No marketplace queries have started yet.</p>'
    );
    return;
  }

  setTextContent(
    timelineNotes.search,
    `Tracking ${searchTasks.length} marketplace quer${searchTasks.length === 1 ? "y" : "ies"} live.`
  );

  setInnerHtml(
    timelineSearchLog,
    visibleSearchTasks
      .map((task) => {
        const query =
          task.output_payload?.search_query || task.input_payload?.search_query || "Waiting for query";
        const comparisonSite =
          task.output_payload?.comparison_site || task.input_payload?.comparison_site || "";
        const candidateCount = task.output_payload?.candidate_count;
        const runtime = task.output_payload?.runtime || {};
        const duration = formatElapsedSeconds(runtime.tinyfish_elapsed_seconds);
        const rightLabel =
          candidateCount !== undefined
            ? `${candidateCount} hit${candidateCount === 1 ? "" : "s"}`
            : progressStateLabels[task.status] || task.status;

        return `
          <div class="search-log-item" data-status="${escapeHtml(task.status)}">
            <div class="search-log-header">
              <span class="search-query">${escapeHtml(query)}</span>
              <span class="candidate-chip">${escapeHtml(rightLabel)}</span>
            </div>
            <p class="search-log-meta">
              ${escapeHtml(formatHostname(comparisonSite))}
              ${duration ? ` · ${escapeHtml(duration)} elapsed` : ""}
              ${describeProviderState(task) ? ` · ${escapeHtml(describeProviderState(task))}` : ""}
            </p>
          </div>
        `;
      })
      .join("")
  );
}

function renderCandidateStage(report) {
  const candidates = collectDiscoveredCandidates(report);
  const visibleCandidates = candidates.slice(0, 4);

  if (candidates.length === 0) {
    setTextContent(
      timelineNotes.candidates,
      "Candidate listings will stream in as search results are captured."
    );
    setInnerHtml(
      timelineCandidateStream,
      '<p class="empty-state">No candidate listings have been captured yet.</p>'
    );
    return;
  }

  setTextContent(
    timelineNotes.candidates,
    `${candidates.length} unique candidate listing${candidates.length === 1 ? "" : "s"} captured so far.`
  );

  setInnerHtml(
    timelineCandidateStream,
    visibleCandidates
      .map((candidate) => {
        const price =
          candidate.price !== null && candidate.price !== undefined
            ? formatCompactCurrency(candidate.price, candidate.currency)
            : "Price unavailable";
        return `
          <article class="candidate-card">
            <div class="candidate-card-head">
              <span class="candidate-marketplace">${escapeHtml(
                candidate.marketplace || formatHostname(candidate.product_url)
              )}</span>
              <span class="candidate-query">${escapeHtml(candidate.discovery_query || "live query")}</span>
            </div>
            <p class="candidate-title">${escapeHtml(
              candidate.title || candidate.model || candidate.product_url
            )}</p>
            <div class="candidate-link">${escapeHtml(candidate.product_url)}</div>
            <div class="candidate-meta">
              <span class="candidate-chip">${escapeHtml(price)}</span>
              <span class="candidate-chip">${escapeHtml(candidate.sku || "No SKU")}</span>
            </div>
          </article>
        `;
      })
      .join("")
  );
}

function getComparisonThreads(report) {
  return collectCompletedComparisons(report).map((comparison) => {
    const fields = [
      ...(comparison.evidence || []).map((item) => item.field),
      ...(comparison.suspicious_signals || []),
      ...(comparison.official_store_signals || []),
    ].filter(Boolean);

    return {
      ...comparison,
      fields: [...new Set(fields)].slice(0, 4),
    };
  });
}

function renderAnalysisStage(report) {
  const threads = getComparisonThreads(report);
  const visibleThreads = threads.slice(0, 1);
  const activeTasks = getComparisonTasks(report).filter(
    (task) => !task.output_payload?.comparison || ["running", "delayed", "failed"].includes(task.status)
  );
  const visibleActiveTasks = activeTasks.slice(0, 3);
  const sourceProduct = report?.extracted_source_product || null;

  if (threads.length === 0) {
    setTextContent(
      timelineNotes.analysis,
      "Comparison signals will assemble here once candidate pages are inspected."
    );
    setInnerHtml(
      timelineSignalGraph,
      `
        <div class="graph-source-node">
          <strong>Source</strong>
          <div>${escapeHtml(sourceProduct?.product_name || "Waiting for extracted source product")}</div>
        </div>
        <p class="empty-state">No comparison graph is available yet.</p>
      `
    );
  } else {
    setTextContent(
      timelineNotes.analysis,
      `Built ${threads.length} reasoning thread${threads.length === 1 ? "" : "s"} from completed comparisons.`
    );
    setInnerHtml(
      timelineSignalGraph,
      `
        <div class="graph-source-node">
          <strong>Source</strong>
          <div>${escapeHtml(
            sourceProduct?.product_name || sourceProduct?.model || report?.source_url || "Source product"
          )}</div>
        </div>
        ${visibleThreads
          .map(
            (thread) => `
              <div class="graph-thread">
                <div class="graph-link">
                  ${(thread.fields || [])
                    .map((field) => `<span class="graph-chip">${escapeHtml(field)}</span>`)
                    .join("")}
                  <span class="graph-arrow">→</span>
                </div>
                <div class="graph-candidate">
                  <strong>${escapeHtml(thread.marketplace || formatHostname(thread.product_url))}</strong>
                  <div>${escapeHtml(
                    thread.candidate_product?.title || thread.candidate_product?.model || thread.product_url
                  )}</div>
                </div>
                <p class="graph-reason">${escapeHtml(thread.reason || "No explanation returned.")}</p>
              </div>
            `
          )
          .join("")}
      `
    );
  }

  if (threads.length === 0 && activeTasks.length === 0) {
    setInnerHtml(
      timelineAnalysisLog,
      '<p class="empty-state">Reasoning traces will appear here once comparison begins.</p>'
    );
    return;
  }

  const logItems =
    threads.length > 0
      ? visibleThreads.map(
          (thread) => `
            <div class="analysis-log-item">
              <div class="analysis-log-head">
                <span class="analysis-log-title">${escapeHtml(
                  thread.candidate_product?.title || thread.product_url
                )}</span>
                <span class="ranking-chip">Risk ${escapeHtml(
                  Number(thread.counterfeit_risk_score || 0).toFixed(1)
                )}</span>
              </div>
              <p class="analysis-log-text">${escapeHtml(thread.reason || "No explanation returned.")}</p>
            </div>
          `
        )
      : visibleActiveTasks.map(
          (task) => `
            <div class="analysis-log-item">
              <div class="analysis-log-head">
                <span class="analysis-log-title">${escapeHtml(
                  formatHostname(task.input_payload?.product_url || "candidate listing")
                )}</span>
                <span class="ranking-chip">${escapeHtml(progressStateLabels[task.status] || task.status)}</span>
              </div>
              <p class="analysis-log-meta">${escapeHtml(
                describeProviderState(task) || "TinyFish is still inspecting this listing."
              )}</p>
            </div>
          `
        );

  setInnerHtml(timelineAnalysisLog, logItems.join(""));
}

function createRankingItem(productUrl) {
  const item = document.createElement("li");
  item.className = "ranking-item";
  item.dataset.productUrl = productUrl;

  const rank = document.createElement("span");
  rank.className = "ranking-rank";
  const main = document.createElement("div");
  main.className = "ranking-main";
  const title = document.createElement("p");
  title.className = "ranking-title";
  const url = document.createElement("a");
  url.className = "ranking-url ranking-url-link";
  url.target = "_blank";
  url.rel = "noreferrer";
  const metadata = document.createElement("div");
  metadata.className = "ranking-metadata";
  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "ranking-toggle";
  toggle.setAttribute("aria-expanded", "false");
  toggle.setAttribute("aria-label", "Show reasoning");
  const chevron = document.createElement("span");
  chevron.className = "ranking-chevron";
  chevron.setAttribute("aria-hidden", "true");
  toggle.appendChild(chevron);
  const detailBody = document.createElement("div");
  detailBody.className = "ranking-details-body";
  detailBody.hidden = true;

  main.append(title, url, metadata);
  item.append(rank, main, toggle, detailBody);
  return item;
}

function renderRankingStage(report) {
  const rankingItems = getRankingSnapshots(report).slice(0, 5);

  if (rankingItems.length === 0) {
    setTextContent(timelineNotes.ranking, "Matches will settle into rank as scores firm up.");
    setInnerHtml(
      timelineRankingList,
      '<li class="empty-state">No ranked matches are available yet.</li>'
    );
    return;
  }

  setTextContent(
    timelineNotes.ranking,
    report?.top_matches?.length
      ? `Ranking finalized across ${rankingItems.length} suspicious match${rankingItems.length === 1 ? "" : "es"}.`
      : `Showing a provisional order from ${rankingItems.length} completed comparison${rankingItems.length === 1 ? "" : "s"}.`
  );

  const previousPositions = new Map(
    [...timelineRankingList.querySelectorAll(".ranking-item")].map((node) => [
      node.dataset.productUrl,
      node.getBoundingClientRect(),
    ])
  );
  const existingItems = new Map(
    [...timelineRankingList.querySelectorAll(".ranking-item")].map((node) => [node.dataset.productUrl, node])
  );
  [...timelineRankingList.querySelectorAll(".empty-state")].forEach((node) => node.remove());

  rankingItems.forEach((match, index) => {
    const productUrl = String(match.product_url);
    let item = existingItems.get(productUrl);
    if (!item) {
      item = createRankingItem(productUrl);
    } else {
      existingItems.delete(productUrl);
    }

    setTextContent(item.querySelector(".ranking-rank"), String(index + 1).padStart(2, "0"));
    setTextContent(
      item.querySelector(".ranking-title"),
      match.candidate_product?.title || match.candidate_product?.model || productUrl
    );
    const urlNode = item.querySelector(".ranking-url");
    urlNode.href = productUrl;
    setTextContent(urlNode, productUrl);
    setInnerHtml(
      item.querySelector(".ranking-metadata"),
      `
        <span class="ranking-metric ranking-metric--risk">
          <span class="ranking-metric-label">Risk</span>
          <span class="ranking-metric-value" style="color: ${escapeHtml(
            getRiskColor(match.counterfeit_risk_score)
          )};">
            ${escapeHtml(Number(match.counterfeit_risk_score || 0).toFixed(1))}
          </span>
        </span>
        <span class="ranking-metric">
          <span class="ranking-metric-label">Match</span>
          <span class="ranking-metric-value">${escapeHtml(Number(match.match_score || 0).toFixed(1))}</span>
        </span>
      `
    );
    setInnerHtml(
      item.querySelector(".ranking-details-body"),
      `
        <p class="ranking-reason">${escapeHtml(match.reason || "No reasoning returned.")}</p>
        <div class="ranking-detail-group">
          <strong>Risk reasoning</strong>
          <ul class="ranking-detail-list">
            ${getRiskReasonLines(match)
              .map((line) => `<li>${escapeHtml(line)}</li>`)
              .join("")}
          </ul>
        </div>
        <div class="ranking-detail-group">
          <strong>Match reasoning</strong>
          <ul class="ranking-detail-list">
            ${getMatchReasonLines(match)
              .map((line) => `<li>${escapeHtml(line)}</li>`)
              .join("")}
          </ul>
        </div>
        ${
          match.suspicious_signals?.length
            ? `
              <div class="ranking-detail-group">
                <strong>Signals</strong>
                <div class="ranking-detail-chips">
                  ${match.suspicious_signals
                    .map((signal) => `<span class="ranking-chip">${escapeHtml(signal)}</span>`)
                    .join("")}
                </div>
              </div>
            `
            : ""
        }
        ${
          match.evidence?.length
            ? `
              <div class="ranking-detail-group">
                <strong>Evidence</strong>
                <div class="ranking-evidence-list">
                  ${match.evidence
                    .slice(0, 3)
                    .map(
                      (evidenceItem) => `
                        <div class="ranking-evidence-item">
                          <span class="ranking-evidence-field">${escapeHtml(evidenceItem.field || "Field")}</span>
                          <p>${escapeHtml(evidenceItem.note || "No note returned.")}</p>
                        </div>
                      `
                    )
                    .join("")}
                </div>
              </div>
            `
            : ""
        }
      `
    );
    const toggleButton = item.querySelector(".ranking-toggle");
    const isOpen = item.classList.contains("is-open");
    toggleButton.setAttribute("aria-expanded", String(isOpen));
    toggleButton.setAttribute("aria-label", isOpen ? "Hide reasoning" : "Show reasoning");
    item.querySelector(".ranking-details-body").hidden = !isOpen;

    timelineRankingList.appendChild(item);
  });

  existingItems.forEach((node) => node.remove());

  requestAnimationFrame(() => {
    [...timelineRankingList.querySelectorAll(".ranking-item")].forEach((node) => {
      const previousPosition = previousPositions.get(node.dataset.productUrl);
      if (!previousPosition) {
        node.animate(
          [{ opacity: 0, transform: "translateY(14px)" }, { opacity: 1, transform: "translateY(0)" }],
          { duration: 420, easing: "cubic-bezier(0.22, 1, 0.36, 1)" }
        );
        return;
      }

      const nextPosition = node.getBoundingClientRect();
      const deltaY = previousPosition.top - nextPosition.top;
      if (deltaY) {
        node.animate(
          [{ transform: `translateY(${deltaY}px)` }, { transform: "translateY(0)" }],
          { duration: 620, easing: "cubic-bezier(0.22, 1, 0.36, 1)" }
        );
      }
    });
  });
}

function renderTimeline(activeReport) {
  renderSourceStage(activeReport);
  renderSearchStage(activeReport);
  renderCandidateStage(activeReport);
  renderAnalysisStage(activeReport);
  renderRankingStage(activeReport);
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
    latestInvestigationPayload = payload;
    lastSubmittedSourceUrl = payload.reports?.[0]?.source_url || lastSubmittedSourceUrl;
    setPhase("progress");
    selectInvestigation(payload.investigation_id);
    upsertPastRunFromInvestigation(payload);
    setStatus(payload.status);
    renderProgressTracking(payload);
    renderResults(payload);
    updateGenerateReportButton(payload);

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
    latestInvestigationPayload = null;
    resetReportScene();
    setPhase("progress");
    setStatus("failed");
    const stepStates = Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "failed"]));
    updateProgressUI({
      overview: "Progress unavailable",
      detail: error.message,
      percent: 0,
      stepStates,
    });
    renderTimeline(null);
    renderEmptyState("The investigation state could not be refreshed. Try again in a moment.");
    updateGenerateReportButton(null);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const source_urls = parseLines(sourceUrlsInput.value);
  const comparison_sites = parseLines(comparisonSitesInput.value);

  if (source_urls.length === 0) {
    setComposerInvalid(true);
    setStatus("idle");
    setPhase("prompt");
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

  setComposerInvalid(false);
  lastSubmittedSourceUrl = source_urls[0] || "";
  latestInvestigationPayload = null;
  resetReportScene();
  updateGenerateReportButton(null);

  if (pollTimer) {
    window.clearTimeout(pollTimer);
  }

  setPhase("progress");
  setStatus("queued");
  const queuedStepStates = Object.fromEntries(
    progressStepDefinitions.map((step, index) => [step.key, index === 0 ? "queued" : "pending"])
  );
  updateProgressUI({
    overview: "Submitting investigation request",
    detail: "Creating the investigation and preparing live progress updates.",
    percent: 4,
    stepStates: queuedStepStates,
  });
  renderTimeline(null);
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
    const stepStates = Object.fromEntries(progressStepDefinitions.map((step) => [step.key, "failed"]));
    updateProgressUI({
      overview: "Investigation failed to start",
      detail: error.message,
      percent: 0,
      stepStates,
    });
    renderTimeline(null);
    renderEmptyState("The investigation could not be started. Check the backend and try again.");
  } finally {
    setSubmitting(false);
  }
});

sourceUrlsInput.addEventListener("input", () => {
  setComposerInvalid(false);
  syncPromptHeight();
});

sourceUrlsInput.addEventListener("focus", () => {
  setComposerInvalid(false);
});

sourceUrlsInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    form.requestSubmit();
  }
});

if (timelineRankingList) {
  timelineRankingList.addEventListener("click", (event) => {
    const toggle = event.target.closest(".ranking-toggle");
    if (!toggle) {
      return;
    }

    const item = toggle.closest(".ranking-item");
    if (!item) {
      return;
    }

    const isOpen = item.classList.toggle("is-open");
    toggle.setAttribute("aria-expanded", String(isOpen));
    toggle.setAttribute("aria-label", isOpen ? "Hide reasoning" : "Show reasoning");

    const detailBody = item.querySelector(".ranking-details-body");
    if (detailBody) {
      detailBody.hidden = !isOpen;
    }
  });
}

if (generateReportButton) {
  generateReportButton.addEventListener("click", async () => {
    if (!latestInvestigationPayload) {
      return;
    }

    reportGenerationInFlight = true;
    updateGenerateReportButton(latestInvestigationPayload);
    setTextContent(progressText, "Generating the styled evidence dossier from the latest investigation data.");

    try {
      presentStyledReport(latestInvestigationPayload);
      setTextContent(progressText, "Styled evidence dossier prepared and embedded below.");
    } catch (error) {
      setTextContent(
        progressText,
        error instanceof Error ? error.message : "The report could not be generated."
      );
    } finally {
      reportGenerationInFlight = false;
      updateGenerateReportButton(latestInvestigationPayload);
    }
  });
}

if (newInvestigationButton) {
  newInvestigationButton.addEventListener("click", () => {
    startNewInvestigation();
  });
}

if (historyButton) {
  historyButton.addEventListener("click", () => {
    const isOpen = historyButton.getAttribute("aria-expanded") === "true";
    setHistoryMenuOpen(!isOpen);
  });
}

if (reportBackButton) {
  reportBackButton.addEventListener("click", () => {
    setPhase("progress");
  });
}

if (reportNewButton) {
  reportNewButton.addEventListener("click", () => {
    startNewInvestigation();
  });
}

if (reportOpenButton) {
  reportOpenButton.addEventListener("click", () => {
    if (!currentReportDocumentUrl) {
      return;
    }
    window.open(currentReportDocumentUrl, "_blank", "noopener");
  });
}

if (pastRunsNode) {
  pastRunsNode.addEventListener("click", (event) => {
    const button = event.target.closest(".past-run-item");
    if (!button) {
      return;
    }
    setHistoryMenuOpen(false);
    loadInvestigation(button.dataset.investigationId);
  });
}

document.addEventListener("click", (event) => {
  if (!historyButton || !historyDropdown) {
    return;
  }

  const target = event.target;
  if (!(target instanceof Node)) {
    return;
  }

  if (historyButton.contains(target) || historyDropdown.contains(target)) {
    return;
  }

  setHistoryMenuOpen(false);
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    setHistoryMenuOpen(false);
  }
});

setStatus("idle");
resetProgressTracking();
renderEmptyState("Add official product page URLs to compare them against live marketplace listings.");
syncPromptHeight();
updateGenerateReportButton(null);
resetReportScene();

currentInvestigationId = getPersistedInvestigationId();
refreshPastRuns();
if (currentInvestigationId) {
  setPhase("progress");
  setStatus("queued");
  const queuedStepStates = Object.fromEntries(
    progressStepDefinitions.map((step, index) => [step.key, index === 0 ? "queued" : "pending"])
  );
  updateProgressUI({
    overview: "Restoring previous investigation",
    detail: "Reloading the latest saved investigation state.",
    percent: 4,
    stepStates: queuedStepStates,
  });
  renderTimeline(null);
  renderEmptyState("Restoring the latest saved investigation state.");
  fetchInvestigation(currentInvestigationId);
} else {
  setPhase("prompt");
}

window.addEventListener("beforeunload", () => {
  revokeCurrentReportDocumentUrl();
});

fetch("/config")
  .then((response) => response.json())
  .then((config) => {
    appConfig = config;
    const stores = (config.ecommerce_store_urls || []).join(", ");
    const lines = [];
    if (config.brand_landing_page_url) {
      lines.push(`Brand home: ${config.brand_landing_page_url}`);
    }
    if (stores) {
      lines.push(`Default marketplace targets: ${stores}`);
      if (comparisonSitesInput && !comparisonSitesInput.value.trim()) {
        comparisonSitesInput.value = (config.ecommerce_store_urls || []).join("\n");
      }
    }
    if (configNote) {
      configNote.textContent =
        lines.join(" • ") ||
        "Environment defaults are not loaded yet. You can still enter source pages and marketplace targets manually.";
    }
  })
  .catch(() => {
    appConfig = null;
    if (configNote) {
      configNote.textContent =
        "Environment defaults could not be loaded. Manual inputs still work.";
    }
  });
