/* The /validate/ page submits straight to datagov-validator's public API
 * (same-origin, proxied by nginx) instead of through this app's own
 * backend, so a slow or malicious submission can't tie up datagov-harvest's
 * own gunicorn workers (see GSA/data.gov#6293). */

const VALIDATOR_API_URL = "/api/v1/validate";
// Takes a pasted or uploaded catalog as the request body itself.
const VALIDATOR_DOCUMENT_URL = "/api/v1/validate/document";
const VALIDATOR_UNAVAILABLE_MESSAGE =
    "The validator is unavailable right now. Please try again later.";

const FIELD_CONFIG = {
    url: { show: "url_field", clear: ["json_text", "json_file"] },
    paste: { show: "json_field", clear: ["url", "json_file"] },
    upload: { show: "upload_field", clear: ["url", "json_text"] },
};

let lastValidationErrors = [];
let lastValidationWarnings = [];

function toggleInputFields() {
    const method = document.getElementById("fetch_method").value;
    const config = FIELD_CONFIG[method];

    Object.values(FIELD_CONFIG).forEach(({ show }) => {
        document.getElementById(show).style.display = "none";
    });

    document.getElementById(config.show).style.display = "block";

    config.clear.forEach(id => {
        document.getElementById(id).value = "";
    });
}

function clearErrors() {
    document.getElementById("validator-results").replaceChildren();
    document.querySelectorAll(".usa-error-message").forEach(el => el.remove());
}

function showFieldError(method, message) {
    const error = document.createElement("span");
    error.className = "usa-error-message";
    error.setAttribute("role", "alert");
    error.textContent = message;
    document.getElementById(FIELD_CONFIG[method].show).appendChild(error);
}

/* ── guards: reject bad input before submitting ── */
function fieldErrorMessage(method) {
    if (method === "upload") {
        const file = document.getElementById("json_file").files[0];
        if (!file) {
            return "A JSON file is required.";
        }
        if (!file.name.toLowerCase().endsWith(".json")) {
            return "Only .json files are accepted.";
        }
        if (file.size > window.MAX_UPLOAD_BYTES) {
            return "File is too large. Maximum size is " + window.MAX_UPLOAD_LABEL + ".";
        }
    } else if (method === "paste") {
        const text = document.getElementById("json_text").value;
        if (!text) {
            return "JSON input is required.";
        }
        if (new Blob([text]).size > window.MAX_UPLOAD_BYTES) {
            return "Pasted JSON is too large. Maximum size is " + window.MAX_UPLOAD_LABEL + ".";
        }
    } else if (!document.getElementById("url").value.trim()) {
        return "URL is required.";
    }
    // a url is fetched by the validator service, which enforces its own limit
    return null;
}

/* ── build the validator API request for the selected fetch method ──
 * A pasted or uploaded catalog goes as the request body itself: never read
 * into a JS string or JSON-escaped, so the browser holds no copy of an
 * uploaded file, and pasted text only as the textarea's value and its UTF-8
 * Blob. The validator decodes a file (UTF-8 or UTF-16) the way this page used
 * to (GSA/data.gov#6460). */
function buildRequest(method) {
    const schema = document.getElementById("schema").value;
    const headers = { "Content-Type": "application/json" };

    if (method === "url") {
        const url = document.getElementById("url").value;
        return [
            VALIDATOR_API_URL,
            { method: "POST", headers, body: JSON.stringify({ schema, fetch_method: "url", url }) },
        ];
    }

    const body = method === "paste"
        ? new Blob([document.getElementById("json_text").value])
        : document.getElementById("json_file").files[0];
    const query = new URLSearchParams({ schema, fetch_method: method });
    return [`${VALIDATOR_DOCUMENT_URL}?${query}`, { method: "POST", headers, body }];
}

/* ── turn a 400/413/422 response body into a message to show the submitter ── */
function extractRefusalMessage(body) {
    if (!body || typeof body !== "object") return null;

    if (typeof body.error === "string") return body.error;

    const fieldErrors = body.detail && body.detail.json;
    if (fieldErrors && typeof fieldErrors === "object") {
        const messages = Object.values(fieldErrors)
            .filter(Array.isArray)
            .flat()
            .filter(message => typeof message === "string");
        if (messages.length) return messages.join(" ");
    }

    return null;
}

/* ── small DOM helpers ── */
function labeledParagraph(label, value) {
    const p = document.createElement("p");
    p.style.wordBreak = "break-all";
    const strong = document.createElement("strong");
    strong.textContent = label;
    p.appendChild(strong);
    p.appendChild(document.createTextNode(" " + value));
    return p;
}

function noticeParagraph(lead, text) {
    const notice = document.createElement("p");
    const strong = document.createElement("strong");
    strong.textContent = lead + " ";
    notice.appendChild(strong);
    notice.appendChild(document.createTextNode(text));
    return notice;
}

function downloadButton(id, label, title, onClick) {
    const button = document.createElement("button");
    button.className = "usa-button";
    button.id = id;
    button.title = title;
    button.setAttribute("aria-label", label);
    button.addEventListener("click", onClick);

    const icon = document.createElement("i");
    icon.className = "fa fa-download margin-right-1";
    icon.setAttribute("aria-hidden", "true");
    button.appendChild(icon);
    button.appendChild(document.createTextNode(label + " (.csv)"));
    return button;
}

function showingFirstTen(total) {
    const p = document.createElement("p");
    const strong = document.createElement("strong");
    strong.textContent = `Showing First 10 of ${total}`;
    p.appendChild(strong);
    return p;
}

function countOf(count, noun) {
    return `${count.toLocaleString("en-US")} ${noun}${count === 1 ? "" : "s"}`;
}

/* ── how much was found, and the limits that bound it ── */
function resultsOverview(errorCount, warningCount) {
    const p = document.createElement("p");
    p.id = "results-overview";
    p.textContent =
        `Found ${countOf(errorCount, "error")} and ${countOf(warningCount, "warning")}. ` +
        `The validator reports up to ${countOf(window.MAX_VALIDATION_ERRORS, "error")} ` +
        `and ${countOf(window.MAX_VALIDATION_WARNINGS, "warning")} for each catalog.`;
    return p;
}

function describeIdentifier(identifier, recordType = "dataset") {
    return typeof identifier === "number"
        ? `${identifier} (${recordType.replace("_", " ")} position)`
        : identifier;
}

/* ── one record and its issues, with the job page's markup (components/error_block.j2) ── */
function recordBlock({ identifier, title, recordType, issues }) {
    const block = document.createElement("div");
    block.className = "error-block";

    const heading = document.createElement("h3");
    heading.className = "word-break-all";
    heading.textContent = identifier;
    block.appendChild(heading);

    block.appendChild(labeledParagraph("Title:", title || "N/A"));
    if (recordType) {
        block.appendChild(labeledParagraph("Record Type:", recordType));
    }

    issues.forEach(({ severity, type, messages }) => {
        const section = document.createElement("div");
        section.className = "issue-section";

        const tagLine = document.createElement("p");
        const tag = document.createElement("span");
        tag.className = `usa-tag issue-tag--${severity}`;
        tag.textContent = severity === "error" ? "Error" : "Warning";
        tagLine.appendChild(tag);
        if (type) {
            tagLine.appendChild(document.createTextNode(`(${type})`));
        }
        section.appendChild(tagLine);

        if (messages.length === 1) {
            section.appendChild(labeledParagraph("Message:", messages[0]));
        } else {
            const p = document.createElement("p");
            const strong = document.createElement("strong");
            strong.textContent = "Message:";
            p.appendChild(strong);
            const ul = document.createElement("ul");
            messages.forEach(message => {
                const li = document.createElement("li");
                li.textContent = message;
                ul.appendChild(li);
            });
            p.appendChild(ul);
            section.appendChild(p);
        }
        block.appendChild(section);
    });

    return block;
}

function issueList(id, blocks) {
    const list = document.createElement("div");
    list.className = "error-list";
    list.id = id;
    blocks.forEach(block => list.appendChild(block));
    return list;
}

/* ── group issues by record, then by type, the way the job page's query does;
 * a record's errors come before its warnings, as in error_block.j2 ── */
function groupIssuesByRecord(errors, warnings) {
    const records = new Map();

    function add(identifier, recordType, issue, title = null) {
        // 1.1 errors carry no record type; everything they name is a dataset
        const key = recordType && recordType !== "dataset"
            ? `${recordType}\u0000${identifier}`
            : identifier;
        if (!records.has(key)) {
            records.set(key, { identifier, title: null, recordType: null, issues: new Map() });
        }
        const record = records.get(key);
        record.title = record.title || title;
        record.recordType = record.recordType || recordType;

        const issueKey = `${issue.severity}\u0000${issue.type || ""}`;
        if (!record.issues.has(issueKey)) {
            record.issues.set(issueKey, { severity: issue.severity, type: issue.type, messages: [] });
        }
        record.issues.get(issueKey).messages.push(issue.message);
    }

    // DCAT-US 3.0 schema errors have no record: their message carries the JSON path
    errors.forEach(([identifier, message]) => add(
        describeIdentifier(identifier) || "Catalog",
        null,
        { severity: "error", type: null, message }
    ));
    warnings.forEach(({ identifier, record_type, warning_type, message, title }) => add(
        describeIdentifier(identifier, record_type),
        record_type,
        { severity: "warning", type: warning_type, message },
        title
    ));

    return Array.from(records.values(), record => ({
        ...record,
        issues: Array.from(record.issues.values()),
    }));
}

/* ── the API's issue_summary, as the job page's record issue type summary ── */
function issueSummaryTable(rows) {
    const table = document.createElement("table");
    table.className = "usa-table usa-table--striped";
    table.id = "issue-summary";

    const caption = document.createElement("caption");
    caption.textContent = "Record issue type summary";
    table.appendChild(caption);

    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    ["Severity", "Error type", "Number of issues"].forEach(text => {
        const th = document.createElement("th");
        th.scope = "col";
        th.textContent = text;
        headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach(({ severity, type, count }) => {
        const row = document.createElement("tr");
        [severity, type, String(count)].forEach(text => {
            const td = document.createElement("td");
            td.textContent = text;
            row.appendChild(td);
        });
        tbody.appendChild(row);
    });
    table.appendChild(tbody);
    return table;
}

/* ── the validator's [identifier, message] error pairs: notices and a CSV ── */
function renderErrors(container, errors, validationIncomplete) {
    const heading = document.createElement("h3");
    heading.textContent = "Errors";
    container.appendChild(heading);

    if (validationIncomplete) {
        container.appendChild(noticeParagraph(
            `Stopped at ${countOf(errors.length, "error")}.`,
            "There may be more. Fix these and validate again to find the rest."
        ));
    }

    if (!errors.length) {
        const p = document.createElement("p");
        p.textContent = "No validation errors found";
        container.appendChild(p);
        return;
    }

    const total = errors.length;
    container.appendChild(downloadButton(
        "btn-download",
        "Download all errors",
        `Download all ${total} error(s) as CSV`,
        downloadValidationErrors
    ));
}

/* ── content-quality warnings: notices and a CSV ── */
function renderWarnings(container, warnings, warningsIncomplete) {
    const heading = document.createElement("h3");
    heading.textContent = "Warnings";
    container.appendChild(heading);

    const intro = document.createElement("p");
    intro.textContent =
        "Warnings don't make a catalog invalid. They are what a harvest of this " +
        "catalog would report as possible data-quality problems.";
    container.appendChild(intro);

    if (warningsIncomplete && warnings.length >= window.MAX_VALIDATION_WARNINGS) {
        container.appendChild(noticeParagraph(
            `Stopped at ${countOf(warnings.length, "warning")}.`,
            "There may be more. Fix these and validate again to see the rest."
        ));
    } else if (warningsIncomplete) {
        // the catalog couldn't be split into records; its schema errors say why
        container.appendChild(noticeParagraph(
            "Not every record was checked for warnings.",
            "Fix the reported errors and validate again to see the rest."
        ));
    }

    if (!warnings.length) {
        const p = document.createElement("p");
        p.textContent = "No warnings found";
        container.appendChild(p);
        return;
    }

    const total = warnings.length;
    container.appendChild(downloadButton(
        "btn-download-warnings",
        "Download all warnings",
        `Download all ${total} warning(s) as CSV`,
        downloadValidationWarnings
    ));
}

/* ── every record with issues, first ten shown, like the job page's record list ── */
function renderRecordIssues(container, errors, warnings) {
    const records = groupIssuesByRecord(errors, warnings);
    if (!records.length) {
        return;
    }

    const heading = document.createElement("h3");
    heading.textContent = "Record issue details";
    container.appendChild(heading);

    if (records.length > 10) {
        container.appendChild(showingFirstTen(countOf(records.length, "record")));
    }
    container.appendChild(issueList("record-issues", records.slice(0, 10).map(recordBlock)));
}

function renderResults(
    errors,
    validationIncomplete = false,
    warnings = [],
    warningsIncomplete = false,
    issueSummary = []
) {
    lastValidationErrors = errors;
    lastValidationWarnings = warnings;

    const container = document.getElementById("validator-results");
    container.replaceChildren();

    const heading = document.createElement("h2");
    heading.textContent = "Validation results";
    container.appendChild(heading);

    container.appendChild(resultsOverview(errors.length, warnings.length));
    if (issueSummary.length) {
        container.appendChild(issueSummaryTable(issueSummary));
    }
    renderErrors(container, errors, validationIncomplete);
    renderWarnings(container, warnings, warningsIncomplete);
    renderRecordIssues(container, errors, warnings);
}

/* ── CSV downloads ── */
function downloadCsv(rows, filename) {
    const csv = rows
        .map(row => row.map(cell => {
            let text = String(cell);
            // keep spreadsheet apps from running a cell as a formula
            if (/^[=+\-@\t\r]/.test(text)) {
                text = "'" + text;
            }
            return '"' + text.replace(/"/g, '""') + '"';
        }).join(","))
        .join("\r\n");

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

function downloadValidationErrors() {
    downloadCsv(
        [["Dataset identifier", "Error"]].concat(lastValidationErrors),
        "validation_errors.csv"
    );
}

function downloadValidationWarnings() {
    downloadCsv(
        [["Identifier", "Record type", "Warning type", "Message"]].concat(
            lastValidationWarnings.map(w => [w.identifier, w.record_type, w.warning_type, w.message])
        ),
        "validation_warnings.csv"
    );
}

// This file loads in <head> (see view_validators.html), before the <form>
// below exists in the DOM - everything that touches a page element has to
// wait for DOMContentLoaded.
document.addEventListener("DOMContentLoaded", function () {
    toggleInputFields();

    document.getElementById("fetch_method").addEventListener("change", toggleInputFields);

    ["fetch_method", "url", "schema", "json_text", "json_file"].forEach(id => {
        document.getElementById(id).addEventListener("change", clearErrors);
    });

    document.getElementById("validator-form").addEventListener("submit", async function (event) {
        event.preventDefault();

        const method = document.getElementById("fetch_method").value;
        const guardMessage = fieldErrorMessage(method);
        clearErrors();

        if (guardMessage) {
            showFieldError(method, guardMessage);
            return;
        }

        // Locked while in flight so the answer can't land beside a field that
        // has since been hidden, or under input that's since been swapped out.
        const form = document.getElementById("validator-form");
        const controls = Array.from(
            form.querySelectorAll("input:not([type=hidden]), select, textarea, button")
        );
        const disabledBeforeSubmit = controls.map(control => control.disabled);
        controls.forEach(control => {
            control.disabled = true;
        });

        try {
            const [url, options] = buildRequest(method);
            let response;
            try {
                response = await fetch(url, options);
            } catch (e) {
                showFieldError(method, VALIDATOR_UNAVAILABLE_MESSAGE);
                return;
            }

            let body;
            try {
                body = await response.json();
            } catch (e) {
                body = null;
            }

            if (response.ok && body && Array.isArray(body.validation_errors)) {
                renderResults(
                    body.validation_errors,
                    body.validation_incomplete === true,
                    Array.isArray(body.validation_warnings) ? body.validation_warnings : [],
                    body.warnings_incomplete === true,
                    Array.isArray(body.issue_summary) ? body.issue_summary : []
                );
                return;
            }

            if ([400, 413, 422, 503].includes(response.status)) {
                // a 413 without a JSON body is nginx's own, refused before the validator saw it
                const fallback = response.status === 413
                    ? "Submission is too large. Maximum size is " + window.MAX_UPLOAD_LABEL + "."
                    : VALIDATOR_UNAVAILABLE_MESSAGE;
                showFieldError(method, extractRefusalMessage(body) ?? fallback);
                return;
            }

            showFieldError(method, VALIDATOR_UNAVAILABLE_MESSAGE);
        } finally {
            controls.forEach((control, index) => {
                control.disabled = disabledBeforeSubmit[index];
            });
        }
    });
});
