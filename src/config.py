

TABLE_CONFIGS = {
    "comments": {
        "columns": "contentid, commentid, userid, text, username",
        "content_column": "text",
        "id_column": "commentid",
        "msg_type": "comment"
    },
    "content": {
        "columns": "contentid, userid, body, username, type",
        "content_column": "body",
        "id_column": "contentid",
        "msg_type": "content"
    }
}

THRESHOLDS = {
    "TOXICITY": 0.7,
    "IDENTITY_ATTACK": 0.5,
    "INSULT": 0.8,
    "PROFANITY": 0.9,
    "THREAT": 0.4
}

BATCH_SIZE = 50


import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = 'neighborly-dev'
MESSAGES_COLLECTION = 'messages'
GROUPS_COLLECTION = 'groups'

AI21_API_KEY = os.getenv("AI21_API_KEY")

OUTPUT_FILE = 'ice_breakers_final.json'

GROUP_ID_FIELD = 'group_id'
MESSAGE_FIELD = 'msg'
GROUP_NAME_FIELD = 'name'


PROMPT_TEMPLATE = """
Analyze the following group chat messages:

{messages}

Based on these messages, generate {num_ice_breakers} fun and engaging ice breaker questions that would be suitable for this group. The ice breakers should be related to the general context of the messages and appeal to the interests shown in these messages. {language_instruction}

Format the output as a numbered list.

Ice Breakers:
1."""


