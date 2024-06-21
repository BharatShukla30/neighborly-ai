import os
import json
import requests
from pymongo import MongoClient
from bson.objectid import ObjectId
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')
PERSPECTIVE_API_KEY = os.getenv('PERSPECTIVE_API_KEY')


client = MongoClient(MONGO_URI)
db = client['neighborly-dev']
messages_collection = db['messages']

def analyze_text(text, api_key):
    url = 'https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze'
    params = {
        'key': api_key
    }
    data = {
        'comment': {'text': text},
        'languages': ['en', 'hi', 'hi-Latn'],
        'requestedAttributes': {
            'TOXICITY': {},
            'IDENTITY_ATTACK': {},
            'INSULT': {},
            'PROFANITY': {},
            'THREAT': {}
        }
    }
    response = requests.post(url, params=params, json=data)

    if response.status_code == 200:
        return response.json().get('attributeScores', {})
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return {}

def flag_message(message, reason):
    return {
        "message_id": str(message['_id']),
        "reason": reason,
        "content": message['msg'],
        "flagged_at": datetime.now().isoformat()
    }


flagged_messages = []


thresholds = {
    "TOXICITY": 0.7,
    "IDENTITY_ATTACK": 0.5,
    "INSULT": 0.8,
    "PROFANITY": 0.9,
    "THREAT": 0.4
}


batch_size = 50
skip = 0

while True:
    
    messages = list(messages_collection.find().skip(skip).limit(batch_size))
    
    
    if len(messages) == 0:
        break
    
    for message in messages:
        analysis_result = analyze_text(message['msg'], PERSPECTIVE_API_KEY)
        for attribute, threshold in thresholds.items():
            score = analysis_result.get(attribute, {}).get('summaryScore', {}).get('value', 0)
            if score > threshold:
                flagged_messages.append(flag_message(message, f'High {attribute}'))
                break
    
    skip += batch_size


with open('flagged_messages_modified2.json', 'w') as outfile:
    json.dump(flagged_messages, outfile, indent=4)

print("Flagged messages have been saved to 'flagged_messages_modified2.json'")
