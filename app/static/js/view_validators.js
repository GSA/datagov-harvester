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
        if (file && !file.name.toLowerCase().endsWith(".json")) {
            return "Only .json files are accepted.";
        }
        if (file && file.size > window.MAX_UPLOAD_BYTES) {
            return "File is too large. Maximum size is " + window.MAX_UPLOAD_LABEL + ".";
        }
    } else if (method === "paste") {
        const text = document.getElementById("json_text").value;
        if (new Blob([text]).size > window.MAX_UPLOAD_BYTES) {
            return "Pasted JSON is too large. Maximum size is " + window.MAX_UPLOAD_LABEL + ".";
        }
    }
    // url is fetched by the validator service, which enforces its own limit
    return null;
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
    const file = document.getElementById("json_file").files[0];
    const json_text = await file.text();
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

/* ── render the validator's [identifier, message] pairs ── */
function renderResults(errors) {
    lastValidationErrors = errors;

    const container = document.getElementById("validator-results");
    container.replaceChildren();

    const heading = document.createElement("h2");
    heading.textContent = "Validation results";
    container.appendChild(heading);

    if (!errors.length) {
        const p = document.createElement("p");
        p.textContent = "No validation errors found";
        container.appendChild(p);
        return;
    }

    const total = errors.length;

    const btnDownload = document.createElement("button");
    btnDownload.className = "usa-button";
    btnDownload.id = "btn-download";
    btnDownload.title = `Download all ${total} error(s) as CSV`;
    btnDownload.setAttribute("aria-label", "Download all errors as CSV");
    btnDownload.addEventListener("click", downloadValidationErrors);

    const icon = document.createElement("i");
    icon.className = "fa fa-download margin-right-1";
    icon.setAttribute("aria-hidden", "true");
    btnDownload.appendChild(icon);
    btnDownload.appendChild(document.createTextNode("Download all errors (.csv)"));
    container.appendChild(btnDownload);

    if (total > 10) {
        const p = document.createElement("p");
        const strong = document.createElement("strong");
        strong.textContent = `Showing First 10 of ${total}`;
        p.appendChild(strong);
        container.appendChild(p);
    }

    const list = document.createElement("div");
    list.className = "error-list";

    errors.slice(0, 10).forEach(([identifier, message]) => {
        const block = document.createElement("div");
        block.className = "error-block";

        const idP = document.createElement("p");
        idP.style.wordBreak = "break-all";
        const idStrong = document.createElement("strong");
        idStrong.textContent = "Identifier:";
        idP.appendChild(idStrong);
        idP.appendChild(
            document.createTextNode(
                " " + (typeof identifier === "number" ? `${identifier} (dataset position)` : identifier)
            )
        );
        block.appendChild(idP);

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

/* ── CSV download of all errors ── */
function downloadValidationErrors() {
    const rows = [["Dataset identifier", "Error"]].concat(lastValidationErrors);
    const csv = rows
        .map(row => row.map(cell => '"' + String(cell).replace(/"/g, '""') + '"').join(","))
        .join("\r\n");

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "validation_errors.csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
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

        const submitButton = document.getElementById("validator-form").querySelector("[type=submit]");
        submitButton.disabled = true;

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
                renderResults(body.validation_errors);
                return;
            }

            if ([400, 413, 422].includes(response.status)) {
                showFieldError(method, extractRefusalMessage(body) ?? VALIDATOR_UNAVAILABLE_MESSAGE);
                return;
            }

            showFieldError(method, VALIDATOR_UNAVAILABLE_MESSAGE);
        } finally {
            submitButton.disabled = false;
        }
    });
});
