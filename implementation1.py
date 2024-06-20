import os
import json
import requests
from pymongo import MongoClient
from bson.objectid import ObjectId
from dotenv import load_dotenv
from datetime import datetime
import time


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
        'languages': ['en','hi','hi-Latn'],
        'requestedAttributes': {
            'TOXICITY': {},
            'IDENTITY_ATTACK': {},
            'INSULT': {},
            'PROFANITY': {},
            'THREAT': {}
        }
    }
    response = requests.post(url, params=params, json=data)

    time.sleep(10)

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


messages = messages_collection.find().limit(100)


flagged_messages = []


for message in messages:
    analysis_result = analyze_text(message['msg'], PERSPECTIVE_API_KEY)
    toxicity_score = analysis_result.get('TOXICITY', {}).get('summaryScore', {}).get('value', 0)
    identity_attack_score = analysis_result.get('IDENTITY_ATTACK', {}).get('summaryScore', {}).get('value', 0)
    insult_score = analysis_result.get('INSULT', {}).get('summaryScore', {}).get('value', 0)
    profanity_score = analysis_result.get('PROFANITY', {}).get('summaryScore', {}).get('value', 0)
    threat_score = analysis_result.get('THREAT', {}).get('summaryScore', {}).get('value', 0)

    if toxicity_score > 0.7:
        flagged_messages.append(flag_message(message, 'High Toxicity'))
    elif identity_attack_score > 0.5:
        flagged_messages.append(flag_message(message, 'Identity Attack'))
    elif insult_score > 0.8:
        flagged_messages.append(flag_message(message, 'Insult'))
    elif profanity_score > 0.9:
        flagged_messages.append(flag_message(message, 'Profanity'))
    elif threat_score > 0.4:
        flagged_messages.append(flag_message(message, 'Threat'))


with open('flagged_messages1.json', 'w') as outfile:
    json.dump(flagged_messages, outfile, indent=4)

print("Flagged messages have been saved to 'flagged_messages1.json'")

