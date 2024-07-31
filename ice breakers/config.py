

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
