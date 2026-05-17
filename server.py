import os
import json
import requests
from flask import Flask, request, Response, stream_with_context
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, static_folder='.', static_url_path='')
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')
if not DEEPSEEK_API_KEY:
    raise RuntimeError("请设置环境变量 DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = 'https://api.deepseek.com/v1/chat/completions'

@app.route('/api/health')
def health():
    return {'status': 'ok'}

@app.route('/api/deepseek/stream', methods=['POST'])
def deepseek_stream():
    data = request.get_json()
    if not data or 'messages' not in data:
        return {'error': '缺少 messages'}, 400

    headers = {
        'Authorization': f'Bearer {DEEPSEEK_API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        'model': data.get('model', 'deepseek-v4-flash'),
        'messages': data['messages'],
        'temperature': data.get('temperature', 0.7),
        'max_tokens': data.get('max_tokens', 2000),
        'stream': True
    }

    r = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, stream=True)
    if r.status_code != 200:
        return {'error': f'DeepSeek API 返回 {r.status_code}'}, 502

    def generate():
        for line in r.iter_lines():
            if line:
                yield line.decode('utf-8') + '\n'
        yield 'data: [DONE]\n\n'

    return Response(stream_with_context(generate()), content_type='text/event-stream')

@app.route('/')
def index():
    return app.send_static_file('olympic_power_shift_3.1_with_country_comparsion.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)