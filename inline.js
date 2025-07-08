function drawScatterCharts(chartData) {
    const chartIds = [
        'b0', 'b0_weighted', 'b0_current', 'if_b0',
        'b1', 'b1_weighted', 'b1_current', 'if_b1'
    ];

    chartIds.forEach(id => {
        const canvas = document.getElementById(id);
        if (!canvas || !chartData[id]) return;

        const values = chartData[id].map(item => ({
            x: item.Date,
            y: item.value
        }));

        const specHigh = chartData[id].map(item => ({
            x: item.Date,
            y: item.upper_spec
        }));

        const specLow = chartData[id].map(item => ({
            x: item.Date,
            y: item.lower_spec
        }));

        new Chart(canvas, {
            type: 'scatter',
            data: {
                datasets: [
                    {
                        label: `${id}`,
                        data: values,
                        backgroundColor: 'blue',
                        showLine: false
                    },
                    {
                        label: None,
                        data: specHigh,
                        type: 'line',
                        borderColor: 'red',
                        borderWidth: 1,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: 'Spec Low',
                        data: specLow,
                        type: 'line',
                        borderColor: 'red',
                        borderWidth: 1,
                        pointRadius: 0,
                        fill: false
                    }
                ]
            },
            options: {
                plugins: {
                    title: {
                        display: true,
                        text: `Chart: ${id}`
                    },
                    legend: {
                        position: 'top'
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        title: {
                            display: true,
                            text: 'Date'
                        },
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm',
                            unit: 'day'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: id
                        },
                        beginAtZero: false
                    }
                }
            }
        });
    });
}

// DOM이 완전히 로드된 후 실행되도록
window.addEventListener('DOMContentLoaded', function () {
    if (window.chartData) {
        drawScatterCharts(window.chartData);
    } else {
        console.warn('chartData not found');
    }
});
