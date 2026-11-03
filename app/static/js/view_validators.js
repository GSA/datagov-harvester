/* The /validate/ page submits straight to datagov-validator's public API
 * (same-origin, proxied by nginx) instead of through this app's own
 * backend, so a slow or malicious submission can't tie up datagov-harvest's
 * own gunicorn workers (see GSA/data.gov#6293). */

const VALIDATOR_API_URL = "/api/v1/validate";
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

/* JSON may be UTF-8, -16 or -32 (RFC 8259); like Python's json.detect_encoding,
 * tell them apart by BOM or by where the nulls fall in the first bytes. Browsers
 * can't decode UTF-32, so those fall through to UTF-8 and fail as invalid JSON. */
function detectEncoding(bytes) {
    if (bytes[0] === 0xfe && bytes[1] === 0xff) return "utf-16be";
    if (bytes[0] === 0xff && bytes[1] === 0xfe) return "utf-16le";
    if (bytes.length >= 2 && bytes[0] === 0 && bytes[1] !== 0) return "utf-16be";
    if (bytes.length >= 2 && bytes[0] !== 0 && bytes[1] === 0) return "utf-16le";
    return "utf-8";
}

async function readJsonFile(file) {
    const bytes = new Uint8Array(await file.arrayBuffer());
    // TextDecoder strips a leading BOM for whichever encoding it's given
    return new TextDecoder(detectEncoding(bytes), { fatal: true }).decode(bytes);
}

/* ── build the validator API payload for the selected fetch method ── */
async function buildPayload(method) {
    const schema = document.getElementById("schema").value;

    if (method === "url") {
        return { schema, fetch_method: "url", url: document.getElementById("url").value };
    }

    if (method === "paste") {
        return {
            schema,
            fetch_method: "paste",
            json_text: document.getElementById("json_text").value,
        };
    }

    // "upload": read the file client-side and send it the same way a paste is sent
    const json_text = await readJsonFile(document.getElementById("json_file").files[0]);
    return { schema, fetch_method: "paste", json_text };
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

function describeIdentifier(identifier, recordType = "dataset") {
    return typeof identifier === "number"
        ? `${identifier} (${recordType.replace("_", " ")} position)`
        : identifier;
}

/* ── render the validator's [identifier, message] error pairs ── */
function renderErrors(container, errors, validationIncomplete) {
    if (validationIncomplete) {
        container.appendChild(noticeParagraph(
            "Validation stopped early.",
            "Fix the reported errors and validate again to find any remaining errors."
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

    if (total > 10) {
        container.appendChild(showingFirstTen(total));
    }

    const list = document.createElement("div");
    list.className = "error-list";

    errors.slice(0, 10).forEach(([identifier, message]) => {
        const block = document.createElement("div");
        block.className = "error-block";
        block.appendChild(labeledParagraph("Identifier:", describeIdentifier(identifier)));

        const msgP = document.createElement("p");
        const msgStrong = document.createElement("strong");
        msgStrong.textContent = "Error Message:";
        msgP.appendChild(msgStrong);
        msgP.appendChild(document.createTextNode(" "));
        const ul = document.createElement("ul");
        const li = document.createElement("li");
        li.textContent = message;
        ul.appendChild(li);
        msgP.appendChild(ul);
        block.appendChild(msgP);

        list.appendChild(block);
    });

    container.appendChild(list);
}

/* ── count warnings per type, most common first ── */
function summarizeWarnings(warnings) {
    const counts = new Map();
    warnings.forEach(({ warning_type }) => {
        counts.set(warning_type, (counts.get(warning_type) || 0) + 1);
    });
    return Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

function warningSummaryTable(warnings) {
    const table = document.createElement("table");
    table.className = "usa-table usa-table--borderless";
    table.id = "warning-summary";

    const caption = document.createElement("caption");
    caption.textContent = "Warnings by type";
    table.appendChild(caption);

    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    ["Warning type", "Count"].forEach(text => {
        const th = document.createElement("th");
        th.scope = "col";
        th.textContent = text;
        headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    summarizeWarnings(warnings).forEach(([type, count]) => {
        const row = document.createElement("tr");
        const typeCell = document.createElement("th");
        typeCell.scope = "row";
        typeCell.textContent = type;
        const countCell = document.createElement("td");
        countCell.textContent = String(count);
        row.appendChild(typeCell);
        row.appendChild(countCell);
        tbody.appendChild(row);
    });
    table.appendChild(tbody);
    return table;
}

/* ── render content-quality warnings: summary, first ten, CSV ── */
function renderWarnings(container, warnings, warningsIncomplete) {
    const heading = document.createElement("h3");
    heading.textContent = "Warnings";
    container.appendChild(heading);

    const intro = document.createElement("p");
    intro.textContent =
        "Warnings don't make a catalog invalid. They are what a harvest of this " +
        "catalog would report as possible data-quality problems.";
    container.appendChild(intro);

    if (warningsIncomplete) {
        container.appendChild(noticeParagraph(
            "Warning checks stopped early.",
            "Fix the reported errors and warnings and validate again to see the rest."
        ));
    }

    if (!warnings.length) {
        const p = document.createElement("p");
        p.textContent = "No warnings found";
        container.appendChild(p);
        return;
    }

    const total = warnings.length;
    container.appendChild(warningSummaryTable(warnings));
    container.appendChild(downloadButton(
        "btn-download-warnings",
        "Download all warnings",
        `Download all ${total} warning(s) as CSV`,
        downloadValidationWarnings
    ));

    if (total > 10) {
        container.appendChild(showingFirstTen(total));
    }

    const list = document.createElement("div");
    list.className = "warning-list";

    warnings.slice(0, 10).forEach(({ identifier, record_type, warning_type, message }) => {
        const block = document.createElement("div");
        block.className = "warning-block";
        block.appendChild(labeledParagraph("Identifier:", describeIdentifier(identifier, record_type)));
        block.appendChild(labeledParagraph("Record type:", record_type));
        block.appendChild(labeledParagraph("Warning type:", warning_type));
        block.appendChild(labeledParagraph("Message:", message));
        list.appendChild(block);
    });

    container.appendChild(list);
}

function renderResults(
    errors,
    validationIncomplete = false,
    warnings = [],
    warningsIncomplete = false
) {
    lastValidationErrors = errors;
    lastValidationWarnings = warnings;

    const container = document.getElementById("validator-results");
    container.replaceChildren();

    const heading = document.createElement("h2");
    heading.textContent = "Validation results";
    container.appendChild(heading);

    renderErrors(container, errors, validationIncomplete);
    renderWarnings(container, warnings, warningsIncomplete);
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
            let payload;
            try {
                payload = await buildPayload(method);
            } catch (e) {
                showFieldError(method, "Could not read the uploaded file.");
                return;
            }

            let response;
            try {
                response = await fetch(VALIDATOR_API_URL, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
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
                    body.warnings_incomplete === true
                );
                return;
            }

            if ([400, 413, 422].includes(response.status)) {
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
