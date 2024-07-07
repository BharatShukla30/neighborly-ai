

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
