from flask import Flask, request, jsonify, render_template
import openai
import json
import requests
import aiohttp
import asyncio
from dotenv import dotenv_values

# Load environment variables from .env file
config = dotenv_values(".env")

app = Flask(__name__)

# Get API keys from environment variables
openai.api_key = config['OPENAI_API_KEY']
typesense_api_key = config['TYPESENSE_API_KEY']
typesense_api_url = config['TYPESENSE_API_URL']

# Assistant IDs remain in the code
assistant_id_1 = 'asst_CIV1OrVIkkiPA2mny72u8TLO'
assistant_id_2 = 'asst_mQ8PhYHrTbEvLjfH8bVXPisQ'

class CustomEventHandler(openai.AssistantEventHandler):
    def __init__(self):
        super().__init__()
        self.response_text = ""

    def on_text_created(self, text) -> None:
        self.response_text = ""

    def on_text_delta(self, delta, snapshot):
        self.response_text += delta.value

    def on_tool_call_created(self, tool_call):
        pass

    def on_tool_call_delta(self, delta, snapshot):
        pass

def call_assistant(assistant_id, user_input, thread_id=None):
    try:
        if thread_id is None:
            thread = openai.beta.threads.create()
            thread_id = thread.id
        else:
            openai.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=user_input
            )
        
        event_handler = CustomEventHandler()

        with openai.beta.threads.runs.stream(
            thread_id=thread_id,
            assistant_id=assistant_id,
            event_handler=event_handler,
        ) as stream:
            stream.until_done()

        return event_handler.response_text, thread_id
    except openai.error.OpenAIError as e:
        return str(e), thread_id
    except Exception as e:
        return str(e), thread_id

def extract_search_query(response):
    search_marker = "SEARCH_QUERY:"
    if search_marker in response:
        start_index = response.find(search_marker) + len(search_marker)
        search_query = response[start_index:].strip()
        return search_query
    return None

def parse_assistant_message(content):
    try:
        parsed_content = json.loads(content)
        return {
            "q": parsed_content.get("q", ""),
            "query_by": parsed_content.get("query_by", ""),
            "collection": parsed_content.get("collection", ""),
            "vector_query": parsed_content.get("vector_query", ""),
            "filter_by": parsed_content.get("filter_by", "")
        }
    except json.JSONDecodeError:
        return None

async def is_url_404(session, url):
    async with session.head(url) as response:
        return response.status == 404

async def check_urls(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [is_url_404(session, url) for url in urls]
        return await asyncio.gather(*tasks)

def perform_typesense_search(params):
    headers = {
        'Content-Type': 'application/json',
        'X-TYPESENSE-API-KEY': typesense_api_key,
    }
    body = {
        "searches": [{
            "q": params["q"],
            "query_by": params["query_by"],
            "collection": params["collection"],
            "prefix": "false",
            "vector_query": params["vector_query"],
            "include_fields": "titel,ppn",
            "per_page": 15,
            "filter_by": params["filter_by"]
        }]
    }

    response = requests.post(typesense_api_url, headers=headers, json=body)
    
    if response.status_code == 200:
        search_results = response.json()
        results = [
            {
                "ppn": hit["document"]["ppn"],
                "titel": hit["document"]["titel"]
            } for hit in search_results["results"][0]["hits"]
        ]

        urls = [f"https://zoeken.oba.nl/resolve.ashx?index=ppn&identifiers={result['ppn']}" for result in results]
        url_checks = asyncio.run(check_urls(urls))
        
        valid_results = [results[i] for i in range(len(results)) if not url_checks[i]]

        simplified_results = {"results": valid_results}
        return simplified_results
    else:
        return {"error": response.status_code, "message": response.text}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_thread', methods=['POST'])
def start_thread():
    try:
        thread = openai.beta.threads.create()
        return jsonify({'thread_id': thread.id})
    except openai.error.OpenAIError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/send_message', methods=['POST'])
def send_message():
    try:
        data = request.json

        thread_id = data['thread_id']
        user_input = data['user_input']
        assistant_id = data['assistant_id']

        response_text, thread_id = call_assistant(assistant_id, user_input, thread_id)
        search_query = extract_search_query(response_text)

        if search_query:
            response_text_2, thread_id = call_assistant(assistant_id_2, search_query, thread_id)
            search_params = parse_assistant_message(response_text_2)
            if search_params:
                search_results = perform_typesense_search(search_params)
                return jsonify({'response': search_results, 'thread_id': thread_id})
            else:
                return jsonify({'response': response_text_2, 'thread_id': thread_id})
        else:
            return jsonify({'response': response_text, 'thread_id': thread_id})
    except openai.error.OpenAIError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/apply_filters', methods=['POST'])
def apply_filters():
    try:
        data = request.json

        thread_id = data['thread_id']
        filter_values = data['filter_values']
        assistant_id = data['assistant_id']

        response_text, thread_id = call_assistant(assistant_id, filter_values, thread_id)
        search_query = extract_search_query(response_text)

        if search_query:
            response_text_2, thread_id = call_assistant(assistant_id_2, search_query, thread_id)
            search_params = parse_assistant_message(response_text_2)
            if search_params:
                search_results = perform_typesense_search(search_params)
                return jsonify({'results': search_results['results'], 'thread_id': thread_id})
            else:
                return jsonify({'response': response_text_2, 'thread_id': thread_id})
        else:
            return jsonify({'response': response_text, 'thread_id': thread_id})
    except openai.error.OpenAIError as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/reset', methods=['POST'])
def reset():
    return jsonify({'status': 'reset'})

if __name__ == '__main__':
    app.run(debug=True)
    