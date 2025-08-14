// Convert UTC dates to local timezone
function convertUTCDatesToLocal(container) {
  // Find all date cells with UTC timestamps within the container (or document if no container)
  const searchRoot = container || document;
  const dateCells = searchRoot.querySelectorAll(".utc-date[data-utc-date]");
  const localFormat = new Intl.DateTimeFormat("en-CA", {
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "shortOffset",
  });

  dateCells.forEach(function (cell) {
    // Skip if already converted (to avoid double conversion)
    if (cell.hasAttribute("data-converted")) return;

    const utcDateString = cell.getAttribute("data-utc-date");
    if (!utcDateString) return;

    const dateObject = new Date(Date.parse(utcDateString));
    if (isNaN(dateObject)) return;

    const parts = localFormat.formatToParts(dateObject);
    const partValues = parts.map((p) => p.value);

    // a.m. to A.M.
    partValues[10] = partValues[10].toUpperCase();
    // put timezone info in parentheses
    partValues[12] = "(" + partValues[12] + ")";

    cell.textContent = partValues.join("").replaceAll(".", "").replace(",", "");

    cell.setAttribute("data-converted", "true");
  });
}

// Convert dates on initial page load
document.addEventListener("DOMContentLoaded", function () {
  convertUTCDatesToLocal();
});

// Convert dates after HTMX content is loaded
document.addEventListener("htmx:afterSwap", function (event) {
  // Convert dates in the newly swapped content
  convertUTCDatesToLocal(event.detail.target);
});

// Also handle htmx:load event for broader compatibility
document.addEventListener("htmx:load", function (event) {
  convertUTCDatesToLocal(event.detail.elt);
});
