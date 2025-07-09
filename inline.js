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

        const yValues = chartData[id].map(item => [
            item.values,
            item.upper_sepc,
            item.lower_spec
        ]).flat().filter(v => typeof v === 'number' && !isNaN(v) && isFinite(v));

        const yMin = Math.min(...yValues);
        const yMax = Math.max(...yValues);
        const yRange = yMax - yMin;
        const yPadding = yRange * 0.3;

        new Chart(canvas, {
            type: 'scatter',
            data: {
                datasets: [
                    {
                        label: `${id}`,
                        data: values,
                        backgroundColor: 'blue',
                        showLine: false,
                        pointRadius : 5

                    },
                    {
                        label: 'Spec High',
                        data: specHigh,
                        type: 'line',
                        borderColor: 'red',
                        borderWidth: 3,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: 'Spec Low',
                        data: specLow,
                        type: 'line',
                        borderColor: 'red',
                        borderWidth: 3,
                        pointRadius: 0,
                        fill: false
                    }
                ]
            },
            options: {
                plugins: {
                    title: {
                        display: true,
                        text: `${id}`,
                        font: {
                            size: 30
                        }
                    },
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        title: {
                            display: true,
                            font: {
                                size: 30
                            }
                        },
                        time: {
                            unit: 'day',
                            tooltipFormat: 'MM-dd',
                        },
                        grid: {
                            display: false
                        },
                        ticks: {
                            font: {
                                size: 20,
                                weight : 'bold'
                            }
                        }
                    },
                    y: {
                        title: {
                            display: true,
                        },
                        grid: {
                            display: false
                        },
                        ticks: { 
                            font: {
                                size: 25,
                                weight : 'bold'
                            }
                        },
                        suggestedMin: yMin - yPadding,
                        suggestedMax: yMax + yPadding
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
