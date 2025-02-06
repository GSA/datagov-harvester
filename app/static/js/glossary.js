var Glossary = require("glossary-panel");
var terms = require("../data/glossary.json");

function decorator(glossary) {
  var accordion = glossary.accordion;

  accordion.opts.collapseOthers = true;
  accordion.collapse = function (button) {
    var content = document.getElementById(button.getAttribute("aria-controls"));
    if (!content) return;
    button.setAttribute("aria-expanded", "false");
    content.setAttribute("aria-hidden", "true");
    this.setStyles(content);
  };
}

// add in source to description.
var adjusted_terms = terms.map(function (t) {
  return {
    term: t.term,
    definition: `<p>${t.definition}</p>`,
  };
});

var g = new Glossary(adjusted_terms);
decorator(g);
