
function filter(e) {
    search = e.value.toLowerCase();
    document.querySelectorAll('.site-component-card').forEach(function (row) {
        text = row.getAttribute("data-meta").toLowerCase();
        if (text.match(search)) {
            row.classList.remove("display-none");
        } else {
            row.classList.add("display-none");
        }
    });
    componentCount = document.querySelectorAll('.site-component-card:not(.display-none)').length;
    // The page tells us what it is listing via data-noun; default to "source".
    var noun = e.getAttribute("data-noun") || "source";
    var word = (componentCount === 1) ? noun : noun + "s";
    var countContainer = document.getElementById("component-count");
    var strongCount = document.createElement("strong");
    strongCount.textContent = componentCount;
    countContainer.replaceChildren(strongCount, document.createTextNode(` ${word} found`));
}

document.addEventListener("DOMContentLoaded", () => {
  // add onkeyup event handlers
  const filterInput = document.getElementById("icon-filter");
  if (filterInput) filterInput.addEventListener("keyup", function (e) {filter(this)});
});
