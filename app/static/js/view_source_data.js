// View Source Data JavaScript functionality

// Confirmation dialog for form submission actions
function confirmSubmit(event, type) {
    let messageEnum = {
        'clear': 'Are you sure you want to clear all datasets?',
        'delete': 'Are you sure you want to delete this source?'
    };
    if (!confirm(messageEnum[type])) {
        event.preventDefault(); // Prevents the form from submitting if the user cancels
    }
}

// Build harvest jobs chart configuration
const buildLineChart = (el, chartData) => {
    let chart = {
        type: 'line',
        data: chartData,
        options: {
            responsive: true,
            plugins: {
                legend: {
                    position: 'top',
                },
                title: {
                    display: true,
                    text: 'Harvest Job History'
                }
            }
        },
    };
    return chart;
}

// Initialize the chart when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    const lineEl = document.getElementById('datagov-line-chart');
    if (lineEl && window.chartData) {
        new Chart(lineEl, buildLineChart(lineEl, window.chartData));
    }
});
