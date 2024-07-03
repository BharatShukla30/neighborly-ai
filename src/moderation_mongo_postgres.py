import os
import json
import requests
import psycopg2
from psycopg2.extras import DictCursor
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import time
from bson import ObjectId

load_dotenv()

POSTGRES_URI = os.getenv('POSTGRES_URI')
MONGO_URI = os.getenv('MONGO_URI')
PERSPECTIVE_API_KEY = os.getenv('PERSPECTIVE_API_KEY')
PERSPECTIVE_API_URL = os.getenv('PERSPECTIVE_API_URL')

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client['neighborly-dev']
messages_collection = mongo_db['messages']


def analyze_text(text, api_key):
    url = PERSPECTIVE_API_URL
    params = {'key': api_key}
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
    time.sleep(1)
    response = requests.post(url, params=params, json=data)
    if response.status_code == 200:
        return response.json().get('attributeScores', {})
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return {}


def calculate_severity(analysis_result):
    max_score = 0
    for attribute in analysis_result:
        score = analysis_result[attribute].get('summaryScore', {}).get('value', 0)
        if score > max_score:
            max_score = score
    
    
    if max_score <= 0.2:
        return 1
    elif max_score <= 0.4:
        return 2
    elif max_score <= 0.6:
        return 3
    elif max_score <= 0.8:
        return 4
    else:
        return 5


def flag_message(contentid, commentid, userid, reason, messageid, group_id, msg_type, severity):
    return {
        "contentid": str(contentid) if isinstance(contentid, ObjectId) else contentid,
        "commentid": str(commentid) if isinstance(commentid, ObjectId) else commentid,
        "userid": str(userid) if isinstance(userid, ObjectId) else userid,
        "report_reason": reason,
        "flagged_at": datetime.now().isoformat(),
        "messageid": str(messageid) if isinstance(messageid, ObjectId) else messageid,
        "group_id": str(group_id) if isinstance(group_id, ObjectId) else group_id,
        "type": msg_type,
        "severity": severity
    }


thresholds = {
    "TOXICITY": 0.7,
    "IDENTITY_ATTACK": 0.5,
    "INSULT": 0.8,
    "PROFANITY": 0.9,
    "THREAT": 0.4
}

batch_size = 50
offset = 0
flagged_messages = []


pg_conn = psycopg2.connect(POSTGRES_URI)
pg_cur = pg_conn.cursor(cursor_factory=DictCursor)

def process_postgres_table(table_name, columns, content_column, id_column, msg_type):
    offset = 0
    while True:
        try:
            pg_cur.execute(f"SELECT {columns} FROM public.{table_name} ORDER BY {id_column} LIMIT %s OFFSET %s", (batch_size, offset))
            messages = pg_cur.fetchall()
            if len(messages) == 0:
                break

            for message in messages:
                contentid = message.get('contentid') if msg_type == "content" else None
                commentid = message.get('commentid') if msg_type == "comment" else None
                userid = message.get('userid')
                content = message[content_column]
                username = message.get('username')
                messageid = None
                group_id = None
                content_flagged = False

                # content
                analysis_result = analyze_text(content, PERSPECTIVE_API_KEY)
                severity = calculate_severity(analysis_result)
                for attribute, threshold in thresholds.items():
                    score = analysis_result.get(attribute, {}).get('summaryScore', {}).get('value', 0)
                    if score > threshold:
                        flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in {content_column}', messageid, group_id, msg_type, severity))
                        content_flagged = True
                        break

                # username 
                if not content_flagged and username:
                    username_analysis_result = analyze_text(username, PERSPECTIVE_API_KEY)
                    severity = calculate_severity(username_analysis_result)
                    for attribute, threshold in thresholds.items():
                        score = username_analysis_result.get(attribute, {}).get('summaryScore', {}).get('value', 0)
                        if score > threshold:
                            flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in username', messageid, group_id, msg_type, severity))
                            break

            offset += batch_size
        except psycopg2.OperationalError as e:
            print(f"Database connection error: {e}")
            time.sleep(5)  


process_postgres_table('comments', 'contentid, commentid, userid, text, username', 'text', 'commentid', 'comment')

process_postgres_table('content', 'contentid, userid, body, username, type', 'body', 'contentid', 'content')

# MongoDB messages
skip = 0
while True:
    messages = list(messages_collection.find().skip(skip).limit(batch_size))
    if len(messages) == 0:
        break

    for message in messages:
        messageid = message['_id']
        msg = message['msg']
        userid = None  # Change to correct field name 
        senderName = message['senderName']
        group_id = message['group_id']
        contentid = None
        commentid = None
        msg_flagged = False

        
        analysis_result_msg = analyze_text(msg, PERSPECTIVE_API_KEY)
        severity = calculate_severity(analysis_result_msg)
        for attribute, threshold in thresholds.items():
            score = analysis_result_msg.get(attribute, {}).get('summaryScore', {}).get('value', 0)
            if score > threshold:
                flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in msg', messageid, group_id, 'msg', severity))
                msg_flagged = True
                break
        #sendername
        if not msg_flagged:
            analysis_result_sender = analyze_text(senderName, PERSPECTIVE_API_KEY)
            severity = calculate_severity(analysis_result_sender)
            for attribute, threshold in thresholds.items():
                score = analysis_result_sender.get(attribute, {}).get('summaryScore', {}).get('value', 0)
                if score > threshold:
                    flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in senderName', messageid, group_id, 'msg', severity))
                    break

    skip += batch_size


pg_cur.close()
pg_conn.close()


output_file = 'final4_messages.json'
with open(output_file, 'w') as outfile:
    json.dump(flagged_messages, outfile, indent=4)

print(f"Flagged messages have been saved to '{output_file}'")
