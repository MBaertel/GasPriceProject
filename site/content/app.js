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
        div.textContent = city.name;

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

    const labels = prices.map(p => new Date(p.timestamp).toLocaleString());
    const values = prices.map(p => p.price);

    if (chart) chart.destroy();

    const ctx = document.getElementById('priceChart').getContext('2d');

    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Fuel Price',
                data: values
            }]
        },
        options: {
            responsive: true
        }
    });
}