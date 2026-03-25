const cityInput = document.getElementById('cityInput');
const stationInput = document.getElementById('stationInput');

const cityResults = document.getElementById('cityResults');
const stationResults = document.getElementById('stationResults');

let selectedCityId = null;
let chart;

function getDateRange(range) {
    const end = new Date();
    const start = new Date();

    if (range === 'week') {
        start.setDate(end.getDate() - 7);
    }

    if (range === 'month') {
        start.setMonth(end.getMonth() - 1);
    }

    if (range === 'year') {
        start.setFullYear(end.getFullYear() - 1);
    }

    return {
        start: start.toISOString(),
        end: end.toISOString()
    };
}

cityInput.addEventListener('input', async () => {
    const q = cityInput.value;

    const res = await fetch(`/cities?q=${q}`);
    const cities = await res.json();

    cityResults.innerHTML = '';

    cities.forEach(city => {
        const div = document.createElement('div');
        div.textContent = `${city.name} - ${city.postal_code}`;

        div.onclick = () => {
            cityInput.value = city.name;
            selectedCityId = city.id;
            cityResults.innerHTML = '';
            stationInput.disabled = false;
            stationInput.value = '';
        };

        cityResults.appendChild(div);
    });
});

stationInput.addEventListener('input', async () => {
    const q = stationInput.value;

    if (!selectedCityId) return;

    const res = await fetch(`/stations?city_id=${selectedCityId}&q=${q}`);
    const stations = await res.json();

    stationResults.innerHTML = '';

    stations.forEach(station => {
        const div = document.createElement('div');
        div.textContent = `${station.name} - ${station.street} ${station.house_number}`;

        div.onclick = () => {
            stationInput.value = div.textContent;
            stationResults.innerHTML = '';
            loadPrices(station.id);
        };

        stationResults.appendChild(div);
    });
});

async function loadPrices(stationId) {
    const range = rangeSelect.value;
    const dates = getDateRange(range);

    const res = await fetch(
        `/prices?station_id=${stationId}&start_time=${dates.start}&end_time=${dates.end}`
    );

    const prices = await res.json();

    // unique sorted timestamps
    const labels = [...new Set(prices.map(p => p.timestamp))]
        .sort()
        .map(ts => new Date(ts));

    // unique fuel types
    const fuelTypes = [...new Set(prices.map(p => p.fuel_type_name))];

    // build datasets
    const datasets = fuelTypes.map(fuelType => {
        const fuelPrices = prices.filter(p => p.fuel_type_name === fuelType);

        return {
            label: fuelType,
            data: fuelPrices.map(p => ({
                x: new Date(p.timestamp),
                y: p.price / 1000
            })),
            pointRadius: 2
        };
    });

    if (chart) chart.destroy();

    const ctx = document.getElementById('priceChart').getContext('2d');

    chart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets
        },
    options: {
        maintainAspectRatio: false,
        responsive: true,
        scales: {
            x: {
                type: 'time',
                time: {
                    unit: range != 'year' ? 'day' : 'week',
                    displayFormats: { day: 'DD' }
                },
                min: dates.start,
                max: dates.end,
                title: { display: true, text: 'Day' }
            },
            y: {
                min: 0,     // start from 0
                max: 3,     // go up to 5 €
                ticks: {stepSize:0.2},
                title: { display: true, text: 'Price (€)' }
            }
        },
        plugins: {
            zoom: {
                pan: {
                    enabled: true,
                    mode: 'xy',
                    limits: {
                        x: {min: dates.start,max: dates.end},
                        y: {min: 0, max: 3}
                    } // allow panning both axes
                },
                zoom: {
                    wheel: {
                        enabled: true, // zoom with mouse wheel
                    },
                    pinch: {
                        enabled: true // zoom with pinch on touch devices
                    },
                    mode: 'xy',
                    limits: {
                        x: {min: dates.start,max: dates.end},
                        y: {min: 0, max: 3}
                    }  // allow zooming both axes
                }
            }
        }
    }
    });
}