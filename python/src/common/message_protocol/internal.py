import json


TYPE_DATA = "data"
TYPE_EOF = "eof"
TYPE_PARTIAL_TOP = "partial_top"
TYPE_FINAL_TOP = "final_top"

ROLE_GATEWAY = "gateway"
ROLE_SUM = "sum"
ROLE_AGGREGATION = "aggregation"
ROLE_JOIN = "join"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def build_message(message_type, query_id, source_role, source_id, payload):
    return {
        "type": message_type,
        "query_id": query_id,
        "source": {
            "role": source_role,
            "id": source_id,
        },
        "payload": payload,
    }


def build_data_message(query_id, fruit, amount):
    return build_message(
        TYPE_DATA,
        query_id,
        ROLE_GATEWAY,
        None,
        {
            "fruit": fruit,
            "amount": amount,
        },
    )


def build_eof_message(query_id, source_role, source_id, visited_sum_ids=None):
    return build_message(
        TYPE_EOF,
        query_id,
        source_role,
        source_id,
        {
            "visited_sum_ids": visited_sum_ids or [],
        },
    )


def build_partial_top_message(query_id, source_id, fruit_top):
    return build_message(
        TYPE_PARTIAL_TOP,
        query_id,
        ROLE_AGGREGATION,
        source_id,
        fruit_top,
    )


def build_final_top_message(query_id, fruit_top):
    return build_message(
        TYPE_FINAL_TOP,
        query_id,
        ROLE_JOIN,
        None,
        fruit_top,
    )


def is_data_message(message):
    return message.get("type") == TYPE_DATA


def is_eof_message(message):
    return message.get("type") == TYPE_EOF


def is_partial_top_message(message):
    return message.get("type") == TYPE_PARTIAL_TOP


def is_final_top_message(message):
    return message.get("type") == TYPE_FINAL_TOP


def get_query_id(message):
    return message["query_id"]


def get_source(message):
    return message["source"]


def get_payload(message):
    return message["payload"]