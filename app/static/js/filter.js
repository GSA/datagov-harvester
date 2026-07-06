
function filter(e) {
    search = e.value.toLowerCase();
    // Match on the style-neutral [data-meta] hook so this works for table rows
    // and card <li>s alike (a shared display class would fight table layout).
    document.querySelectorAll('[data-meta]').forEach(function (row) {
        text = row.getAttribute("data-meta").toLowerCase();
        if (text.match(search)) {
            row.classList.remove("display-none");
        } else {
            row.classList.add("display-none");
        }
    });
    componentCount = document.querySelectorAll('[data-meta]:not(.display-none)').length;
    // The page tells us what it is listing via data-noun; default to "source".
    var noun = e.getAttribute("data-noun") || "source";
    var word = (componentCount === 1) ? noun : noun + "s";
    var countContainer = document.getElementById("component-count");
    var strongCount = document.createElement("strong");
    strongCount.textContent = componentCount;
    countContainer.replaceChildren(strongCount, document.createTextNode(` ${word} found`));
}

document.addEventListener("DOMContentLoaded", () => {
  const filterInput = document.getElementById("icon-filter");
  if (!filterInput) return;

  filterInput.addEventListener("keyup", function () { filter(this); });

  const filterForm = filterInput.closest("form");
  if (filterForm) {
    filterForm.addEventListener("submit", function (e) {
      e.preventDefault();
      filter(filterInput);
    });
  }
});
