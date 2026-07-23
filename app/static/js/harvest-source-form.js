function initCollectionParentUrlToggle() {
  const sourceType = document.getElementById("source_type");
  const parentGroup = document.querySelector("[data-collection-parent-url-group]");
  const parentInput = document.getElementById("collection_parent_url");

  if (!sourceType || !parentGroup) {
    return;
  }

  function toggleCollectionParentUrl() {
    const show = sourceType.value === "waf-collection";
    parentGroup.hidden = !show;
    if (parentInput) {
      parentInput.disabled = !show;
    }
  }

  toggleCollectionParentUrl();
  sourceType.addEventListener("change", toggleCollectionParentUrl);
}

document.addEventListener("DOMContentLoaded", initCollectionParentUrlToggle);
