// View Source Data JavaScript functionality

// Confirmation dialog for form submission actions
function confirmSubmit(event) {
    const type = event.target.getAttribute("data-action");
    let messageEnum = {
        'delete': 'Are you sure you want to delete this organization?'
    };
    if (!confirm(messageEnum[type])) {
        event.preventDefault(); // Prevents the form from submitting if the user cancels
    }
}

// Initialize the chart when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    var confirmationElts = document.getElementsByClassName("confirm-submit");
    for (let elt of confirmationElts) {
      elt.addEventListener("click", function (e) {confirmSubmit(e)});
    }
});
