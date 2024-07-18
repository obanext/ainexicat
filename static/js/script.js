let thread_id = null;
let timeoutHandle = null;

function checkInput() {
    const userInput = document.getElementById('user-input').value.trim();
    const sendButton = document.getElementById('send-button');
    const applyFiltersButton = document.getElementById('apply-filters-button');
    const checkboxes = document.querySelectorAll('#filters input[type="checkbox"]');
    let anyChecked = Array.from(checkboxes).some(checkbox => checkbox.checked);

    if (userInput === "") {
        sendButton.disabled = true;
        sendButton.style.backgroundColor = "#ccc";
        sendButton.style.cursor = "not-allowed";
    } else {
        sendButton.disabled = false;
        sendButton.style.backgroundColor = "#6d5ab0";
        sendButton.style.cursor = "pointer";
    }

    if (!anyChecked) {
        applyFiltersButton.disabled = true;
        applyFiltersButton.style.backgroundColor = "#ccc";
        applyFiltersButton.style.cursor = "not-allowed";
    } else {
        applyFiltersButton.disabled = false;
        applyFiltersButton.style.backgroundColor = "#6d5ab0";
        applyFiltersButton.style.cursor = "pointer";
    }
}

async function startThread() {
    const response = await fetch('/start_thread', { method: 'POST' });
    const data = await response.json();
    thread_id = data.thread_id;
}

async function sendMessage() {
    const userInput = document.getElementById('user-input').value.trim();

    if (userInput === "") {
        return;
    }

    displayUserMessage(userInput);
    showLoader();
    
    const sendButton = document.getElementById('send-button');
    document.getElementById('user-input').value = '';
    sendButton.disabled = true;
    sendButton.style.backgroundColor = "#ccc";
    sendButton.style.cursor = "not-allowed";

    timeoutHandle = setTimeout(() => {
        displayAssistantMessage('😿 er is iets misgegaan, we beginnen opnieuw!');
        hideLoader();
        resetThread();
    }, 15000);

    try {
        const response = await fetch('/send_message', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                thread_id: thread_id,
                user_input: userInput,
                assistant_id: 'asst_ejPRaNkIhjPpNHDHCnoI5zKY'
            })
        });
        if (!response.ok) {
            const errorData = await response.json();
            console.error('Error:', errorData.error);
            displayAssistantMessage('😿 er is iets misgegaan, we beginnen opnieuw!');
            hideLoader();
            clearTimeout(timeoutHandle);
            resetThread();
            return;
        }
        const data = await response.json();
        hideLoader();
        clearTimeout(timeoutHandle);

        if (!data.response.results) {
            displayAssistantMessage(data.response);
        }

        if (data.thread_id) {
            thread_id = data.thread_id;
        }

        if (data.response.results) {
            displaySearchResults(data.response.results);
            await sendStatusKlaar();
        }
        
        resetFilters();
    } catch (error) {
        console.error('Unexpected error:', error);
        displayAssistantMessage('😿 er is iets misgegaan, we beginnen opnieuw!');
        hideLoader();
        clearTimeout(timeoutHandle);
        resetThread();
    }

    checkInput();
    scrollToBottom();
}

function resetThread() {
    startThread();
    document.getElementById('messages').innerHTML = '';
    document.getElementById('search-results').innerHTML = '';
    document.getElementById('user-input').placeholder = "Welk boek zoek je? Of informatie over..?";
    addOpeningMessage();
    addPlaceholders();
    scrollToBottom();
    
    resetFilters();
}


async function sendStatusKlaar() {
    try {
        const response = await fetch('/send_message', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                thread_id: thread_id,
                user_input: 'STATUS : KLAAR',
                assistant_id: 'asst_ejPRaNkIhjPpNHDHCnoI5zKY'
            })
        });
        if (!response.ok) {
            const errorData = await response.json();
            console.error('Error:', errorData.error);
            return;
        }
        const data = await response.json();
        displayAssistantMessage(data.response);
        scrollToBottom();
    } catch (error) {
        console.error('Unexpected error:', error);
    }
}

function displayUserMessage(message) {
    const messageContainer = document.getElementById('messages');
    const messageElement = document.createElement('div');
    messageElement.classList.add('user-message');
    messageElement.textContent = message;
    messageContainer.appendChild(messageElement);
    scrollToBottom();
}

function displayAssistantMessage(message) {
    const messageContainer = document.getElementById('messages');
    const messageElement = document.createElement('div');
    messageElement.classList.add('assistant-message');
    if (typeof message === 'object') {
        messageElement.textContent = JSON.stringify(message);
    } else {
        messageElement.textContent = message;
    }
    messageContainer.appendChild(messageElement);
    scrollToBottom();
}

function displaySearchResults(results) {
    const searchResultsContainer = document.getElementById('search-results');
    searchResultsContainer.innerHTML = '';
    results.forEach(result => {
        const resultElement = document.createElement('div');
        resultElement.innerHTML = `
            <a href="https://zoeken.oba.nl/resolve.ashx?index=ppn&identifiers=${result.ppn}" target="_blank">
                <img src="https://cover.biblion.nl/coverlist.dll/?doctype=morebutton&bibliotheek=oba&style=0&ppn=${result.ppn}&isbn=&lid=&aut=&ti=&size=150" alt="Cover for PPN ${result.ppn}">
                <p>${result.titel}</p>
            </a>
        `;
        searchResultsContainer.appendChild(resultElement);
    });
}

async function applyFiltersAndSend() {
    const checkboxes = document.querySelectorAll('#filters input[type="checkbox"]');
    let selectedFilters = [];
    checkboxes.forEach(checkbox => {
        if (checkbox.checked) {
            selectedFilters.push(checkbox.value);
        }
    });
    const filterString = selectedFilters.join('||');

    if (filterString === "") {
        return;
    }

    displayUserMessage(`Filters toegepast: ${filterString}`);
    showLoader();

    try {
        const response = await fetch('/apply_filters', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                thread_id: thread_id,
                filter_values: filterString,
                assistant_id: 'asst_ejPRaNkIhjPpNHDHCnoI5zKY'
            })
        });

        if (!response.ok) {
            const errorData = await response.json();
            console.error('Error:', errorData.error);
            hideLoader();
            return;
        }

        const data = await response.json();
        hideLoader();

        if (data.results) {
            displaySearchResults(data.results);
            await sendStatusKlaar();
        }

        if (data.thread_id) {
            thread_id = data.thread_id;
        }
        
        resetFilters();
    } catch (error) {
        console.error('Unexpected error:', error);
        hideLoader();
    }

    checkInput();
}

function startNewChat() {
    startThread();
    document.getElementById('messages').innerHTML = '';
    document.getElementById('search-results').innerHTML = '';
    document.getElementById('user-input').placeholder = "Welk boek zoek je? Of informatie over..?";
    addOpeningMessage();
    addPlaceholders();
    scrollToBottom();
    
    resetFilters();
}

function extractSearchQuery(response) {
    const searchMarker = "SEARCH_QUERY:";
    if (response.includes(searchMarker)) {
        return response.split(searchMarker)[1].trim();
    }
    return null;
}

function resetFilters() {
    const checkboxes = document.querySelectorAll('#filters input[type="checkbox"]');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });
    checkInput();
}

function showLoader() {
    const messageContainer = document.getElementById('messages');
    const loaderElement = document.createElement('div');
    loaderElement.classList.add('assistant-message', 'loader');
    loaderElement.id = 'loader';
    loaderElement.innerHTML = '<span class="dot">.</span><span class="dot">.</span><span class="dot">.</span>';
    messageContainer.appendChild(loaderElement);
    scrollToBottom();
}

function hideLoader() {
    const loaderElement = document.getElementById('loader');
    if (loaderElement) {
        loaderElement.remove();
    }

    const sendButton = document.getElementById('send-button');
    sendButton.disabled = false;
    sendButton.style.backgroundColor = "#6d5ab0";
    sendButton.style.cursor = "pointer";
}

function scrollToBottom() {
    const messageContainer = document.getElementById('messages');
    messageContainer.scrollTop = messageContainer.scrollHeight;
}

function addOpeningMessage() {
    const openingMessage = "Hoi! Ik ben Nexi, ik help je zoeken naar boeken en informatie in de OBA. Bijvoorbeeld: 'boeken die lijken op Wereldspionnen' of 'heb je informatie over zeezoogdieren?'"
    const messageContainer = document.getElementById('messages');
    const messageElement = document.createElement('div');
    messageElement.classList.add('assistant-message');
    messageElement.textContent = openingMessage;
    messageContainer.appendChild(messageElement);
    scrollToBottom();
}

function addPlaceholders() {
    const searchResultsContainer = document.getElementById('search-results');
    searchResultsContainer.innerHTML = `
        <div><img src="/static/images/placeholder.png" alt="Placeholder"></div>
        <div><img src="/static/images/placeholder.png" alt="Placeholder"></div>
        <div><img src="/static/images/placeholder.png" alt="Placeholder"></div>
        <div><img src="/static/images/placeholder.png" alt="Placeholder"></div>
    `;
}

document.getElementById('user-input').addEventListener('input', function() {
    checkInput();
    if (this.value !== "") {
        this.placeholder = "";
    }
});
document.getElementById('user-input').addEventListener('keypress', function(event) {
    if (event.key === 'Enter') {
        sendMessage();
    }
});
document.querySelectorAll('#filters input[type="checkbox"]').forEach(checkbox => {
    checkbox.addEventListener('change', checkInput);
});

window.onload = () => {
    startThread().then(() => {
        addOpeningMessage();
        addPlaceholders();
        checkInput();
        document.getElementById('user-input').placeholder = "Vertel me wat je zoekt!";
    });

    const applyFiltersButton = document.querySelector('button[onclick="applyFiltersAndSend()"]');
    if (applyFiltersButton) {
        applyFiltersButton.onclick = applyFiltersAndSend;
    }
    
    resetFilters();
};
