document.querySelectorAll(".js-account-menu-toggle").forEach(function (btn) {
  var menu = document.getElementById(btn.getAttribute("aria-controls"));
  if (!menu) return;

  function close() {
    btn.setAttribute("aria-expanded", "false");
    menu.hidden = true;
  }

  function open() {
    btn.setAttribute("aria-expanded", "true");
    menu.hidden = false;
  }

  btn.addEventListener("click", function (e) {
    e.stopPropagation();
    if (menu.hidden) {
      open();
    } else {
      close();
    }
  });

  document.addEventListener("click", function (e) {
    if (!btn.contains(e.target) && !menu.contains(e.target)) {
      close();
    }
  });

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") {
      close();
    }
  });
});
