
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
from config import TABLE_CONFIGS, THRESHOLDS, BATCH_SIZE

load_dotenv()

POSTGRES_URI = os.getenv('POSTGRES_URI1')
MONGO_URI = os.getenv('MONGO_URI')
PERSPECTIVE_API_KEY = os.getenv('PERSPECTIVE_API_KEY')
PERSPECTIVE_API_URL = os.getenv('PERSPECTIVE_API_URL')

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client['neighborly-dev']
messages_collection = mongo_db['messages']
users_collection = mongo_db['users']


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
    max_score = max(
        analysis_result.get(attr, {}).get('summaryScore', {}).get('value', 0)
        for attr in THRESHOLDS
    )
    if max_score >= 0.9:
        return 5
    elif max_score >= 0.7:
        return 4
    elif max_score >= 0.5:
        return 3
    elif max_score >= 0.3:
        return 2
    else:
        return 1

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

flagged_messages = []


pg_conn = psycopg2.connect(POSTGRES_URI)
pg_cur = pg_conn.cursor(cursor_factory=DictCursor)

def process_postgres_table(table_name, config):
    offset = 0
    while True:
        try:
            pg_cur.execute(f"SELECT {config['columns']} FROM public.{table_name} ORDER BY {config['id_column']} LIMIT %s OFFSET %s", (BATCH_SIZE, offset))
            messages = pg_cur.fetchall()
            if len(messages) == 0:
                break

            for message in messages:
                contentid = message.get('contentid') if config['msg_type'] == "content" else None
                commentid = message.get('commentid') if config['msg_type'] == "comment" else None
                userid = message.get('userid')
                content = message[config['content_column']]
                username = message.get('username')
                messageid = None
                group_id = None
                content_flagged = False

                
                analysis_result = analyze_text(content, PERSPECTIVE_API_KEY)
                severity = calculate_severity(analysis_result)  # Calculate severity
                for attribute, threshold in THRESHOLDS.items():
                    score = analysis_result.get(attribute, {}).get('summaryScore', {}).get('value', 0)
                    if score > threshold:
                        flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in {config["content_column"]}', messageid, group_id, config['msg_type'], severity))
                        content_flagged = True
                        break

                
                if not content_flagged and username:
                    username_analysis_result = analyze_text(username, PERSPECTIVE_API_KEY)
                    severity = calculate_severity(username_analysis_result)  # Calculate severity
                    for attribute, threshold in THRESHOLDS.items():
                        score = username_analysis_result.get(attribute, {}).get('summaryScore', {}).get('value', 0)
                        if score > threshold:
                            flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in username', messageid, group_id, config['msg_type'], severity))
                            break

            offset += BATCH_SIZE
        except psycopg2.OperationalError as e:
            print(f"Database connection error: {e}")
            time.sleep(5)  


for table_name, config in TABLE_CONFIGS.items():
    process_postgres_table(table_name, config)

# MongoDB messages
skip = 0
while True:
    messages = list(messages_collection.find().skip(skip).limit(BATCH_SIZE))
    if len(messages) == 0:
        break

    for message in messages:
        messageid = message['_id']
        msg = message['msg']
        senderName = message['senderName']
        
        
        user = users_collection.find_one({"senderName": senderName})
        userid = user['_id'] if user else 0
        
        group_id = message['group_id']
        contentid = None
        commentid = None
        msg_flagged = False

        
        analysis_result_msg = analyze_text(msg, PERSPECTIVE_API_KEY)
        severity = calculate_severity(analysis_result_msg)  # Calculate severity
        for attribute, threshold in THRESHOLDS.items():
            score = analysis_result_msg.get(attribute, {}).get('summaryScore', {}).get('value', 0)
            if score > threshold:
                flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in msg', messageid, group_id, 'msg', severity))
                msg_flagged = True
                break

        
        if not msg_flagged:
            analysis_result_sender = analyze_text(senderName, PERSPECTIVE_API_KEY)
            severity = calculate_severity(analysis_result_sender)  # Calculate severity
            for attribute, threshold in THRESHOLDS.items():
                score = analysis_result_sender.get(attribute, {}).get('summaryScore', {}).get('value', 0)
                if score > threshold:
                    flagged_messages.append(flag_message(contentid, commentid, userid, f'{attribute} in senderName', messageid, group_id, 'msg', severity))
                    break

    skip += BATCH_SIZE


pg_cur.close()

output_file = 'final4_messages.json'
with open(output_file, 'w') as outfile:
    json.dump(flagged_messages, outfile, indent=4)

print(f"Flagged messages have been saved to '{output_file}'")


pg_conn = psycopg2.connect(POSTGRES_URI)  
pg_cur = pg_conn.cursor()

for report in flagged_messages:
    pg_cur.execute("""
        INSERT INTO public.reports (contentid, commentid, userid, report_reason, createdat, processed, messageid, groupid, severity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        report['contentid'],
        report['commentid'],
        report['userid'],
        report['report_reason'],
        report['flagged_at'],
        False,  # Default value for processed
        report['messageid'],
        report['group_id'],
        report['severity']
    ))


pg_conn.commit()
pg_cur.close()
pg_conn.close()

print("Flagged messages have been inserted into the reports table.")