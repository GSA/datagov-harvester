// View Source Data JavaScript functionality

// Confirmation dialog for form submission actions
function confirmSubmit(event) {
    const type = event.target.getAttribute("data-action");
    let messageEnum = {
        'clear': 'Are you sure you want to clear all datasets?',
        'delete': 'Are you sure you want to delete this source? Deletion may take some time.'
    };
    if (!confirm(messageEnum[type])) {
        event.preventDefault(); // Prevents the form from submitting if the user cancels
    }
}

// Build harvest jobs chart configuration
const buildHarvestJobChart = (el, chartData) => {
    const compactData = {
        labels: chartData.labels,
        datasets: chartData.datasets.map((dataset) => ({
            ...dataset,
            borderWidth: 0,
            barPercentage: 0.85,
            categoryPercentage: 0.8,
        })),
    };

    return {
        type: 'bar',
        data: compactData,
        options: {
            maintainAspectRatio: false,
            responsive: true,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            scales: {
                x: {
                    stacked: true,
                    grid: {
                        display: false,
                    },
                    ticks: {
                        autoSkip: true,
                        maxRotation: 0,
                        maxTicksLimit: 8,
                    },
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Record Count',
                    },
                    grid: {
                        color: 'rgba(0, 0, 0, 0.08)',
                    },
                    ticks: {
                        precision: 0,
                    },
                },
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        boxHeight: 10,
                        boxWidth: 18,
                        padding: 12,
                    },
                },
                title: {
                    display: false,
                },
            },
        },
    };
}

// Initialize the chart when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    const lineEl = document.getElementById('datagov-line-chart');
    if (lineEl && window.chartData) {
        new Chart(lineEl, buildHarvestJobChart(lineEl, window.chartData));
    }

    var confirmationElts = document.getElementsByClassName("confirm-submit");
    for (let elt of confirmationElts) {
      elt.addEventListener("click", function (e) {confirmSubmit(e)});
    }
});
