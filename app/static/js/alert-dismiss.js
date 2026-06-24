document.querySelectorAll(".js-alert-close").forEach(function (btn) {
  btn.addEventListener("click", function () {
    btn.closest(".usa-alert")?.remove();
  });
});
