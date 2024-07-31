import os
from pymongo import MongoClient
from datetime import datetime
import requests
import json
from collections import defaultdict
from bson import ObjectId
import config

client = MongoClient(config.MONGO_URI)
db = client[config.DATABASE_NAME]
messages_collection = db[config.MESSAGES_COLLECTION]
groups_collection = db[config.GROUPS_COLLECTION]

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

def fetch_and_group_messages():
    messages_cursor = messages_collection.find({})
    grouped_messages = defaultdict(list)
    
    for message in messages_cursor:
        group_id = message[config.GROUP_ID_FIELD]
        msg_content = message[config.MESSAGE_FIELD]
        if msg_content: 
            grouped_messages[group_id].append(msg_content)
    
    return grouped_messages

def fetch_group_name(group_id):
    group = groups_collection.find_one({"_id": group_id})
    return group[config.GROUP_NAME_FIELD] if group else "Unknown Group"

def determine_language_preference(messages):
    hindi_word_count = sum(1 for msg in messages if any(char.isalpha() and ord(char) > 255 for char in msg))
    return 'hinglish' if hindi_word_count / len(messages) > 0.7 else 'english'

def generate_group_ice_breakers(messages, num_ice_breakers=5):
    combined_messages = "\n".join(messages)
    language_preference = determine_language_preference(messages)
    
    if language_preference == 'hinglish':
        language_instruction = "Include a mix of questions in English and Hindi (Hinglish)."
    else:
        language_instruction = "Generate questions in English and Hinglish"
    
    prompt = config.PROMPT_TEMPLATE.format(
        messages=combined_messages,
        num_ice_breakers=num_ice_breakers,
        language_instruction=language_instruction
    )

    headers = {
        "Authorization": f"Bearer {config.AI21_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "prompt": prompt,
        "numResults": 1,
        "maxTokens": 300,
        "temperature": 0.7,
        "topKReturn": 0,
        "stopSequences": ["\n\n"]
    }

    response = requests.post(
        "https://api.ai21.com/studio/v1/j2-ultra/complete",
        headers=headers,
        json=payload
    )

    if response.status_code == 200:
        response_json = response.json()
        generated_text = response_json['completions'][0]['data']['text']
        return "1." + generated_text.strip()
    else:
        return f"Error: {response.status_code}, {response.text}"

def save_ice_breakers_to_file(ice_breakers_data):
    with open(config.OUTPUT_FILE, 'w') as f:
        json.dump(ice_breakers_data, f, indent=2, cls=JSONEncoder)

def main():
    grouped_messages = fetch_and_group_messages()
    ice_breakers_data = {}
    
    for group_id, messages in grouped_messages.items():
        print(f"Generating ice breakers for group: {group_id}")
        ice_breakers = generate_group_ice_breakers(messages)
        group_name = fetch_group_name(group_id)
        print(f"Ice Breakers for group {group_id}:")
        print(ice_breakers)
        print("\n")
        
        ice_breakers_data[f'group_id {group_id}'] = {
            'name': group_name,
            'ice_breakers': ice_breakers
        }
    
    save_ice_breakers_to_file(ice_breakers_data)
    print(f"Ice breakers have been saved to {config.OUTPUT_FILE}")

if __name__ == "__main__":
    main()
