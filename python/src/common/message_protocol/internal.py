import json


TYPE_DATA = "data"
TYPE_EOF = "eof"
TYPE_PARTIAL_TOP = "partial_top"
TYPE_FINAL_TOP = "final_top"
TYPE_CLOSE_SIGNAL = "close_signal"
TYPE_COUNT_REPLY = "count_reply"
TYPE_RECHECK_SIGNAL = "recheck_signal"
TYPE_GO_SIGNAL = "go_signal"

ROLE_GATEWAY = "gateway"
ROLE_SUM = "sum"
ROLE_AGGREGATION = "aggregation"
ROLE_JOIN = "join"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    decoded_message = json.loads(message.decode("utf-8"))
    assert isinstance(decoded_message, dict)
    assert "type" in decoded_message
    assert "query_id" in decoded_message
    assert "source" in decoded_message
    assert "payload" in decoded_message
    return decoded_message


def build_message(message_type, query_id, source_role, source_id, payload):
    assert message_type in {
        TYPE_DATA,
        TYPE_EOF,
        TYPE_PARTIAL_TOP,
        TYPE_FINAL_TOP,
        TYPE_CLOSE_SIGNAL,
        TYPE_COUNT_REPLY,
        TYPE_RECHECK_SIGNAL,
        TYPE_GO_SIGNAL,
    }
    assert source_role in {
        ROLE_GATEWAY,
        ROLE_SUM,
        ROLE_AGGREGATION,
        ROLE_JOIN,
    }
    assert isinstance(query_id, str)
    assert query_id

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
    assert isinstance(fruit, str)
    assert fruit
    assert isinstance(amount, int)
    assert amount >= 0

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


def build_eof_message(query_id, source_role, source_id, total_sent=None):
    payload = {}
    if total_sent is not None:
        payload["total_sent"] = total_sent
    return build_message(TYPE_EOF, query_id, source_role, source_id, payload)


def build_close_signal_message(query_id, source_id, total_expected):
    return build_message(
        TYPE_CLOSE_SIGNAL,
        query_id,
        ROLE_SUM,
        source_id,
        {"total_expected": total_expected},
    )


def build_count_reply_message(query_id, source_id, count):
    assert isinstance(count, int)
    assert count >= 0
    return build_message(
        TYPE_COUNT_REPLY,
        query_id,
        ROLE_SUM,
        source_id,
        {"count": count},
    )


def build_recheck_signal_message(query_id, source_id):
    return build_message(TYPE_RECHECK_SIGNAL, query_id, ROLE_SUM, source_id, {})


def build_go_signal_message(query_id, source_id):
    return build_message(TYPE_GO_SIGNAL, query_id, ROLE_SUM, source_id, {})


def build_partial_top_message(query_id, source_id, fruit_top):
    assert isinstance(source_id, int)
    assert isinstance(fruit_top, list)

    return build_message(
        TYPE_PARTIAL_TOP,
        query_id,
        ROLE_AGGREGATION,
        source_id,
        fruit_top,
    )


def build_final_top_message(query_id, fruit_top):
    assert isinstance(fruit_top, list)

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


def is_close_signal_message(message):
    return message.get("type") == TYPE_CLOSE_SIGNAL


def is_count_reply_message(message):
    return message.get("type") == TYPE_COUNT_REPLY


def is_recheck_signal_message(message):
    return message.get("type") == TYPE_RECHECK_SIGNAL


def is_go_signal_message(message):
    return message.get("type") == TYPE_GO_SIGNAL


def get_query_id(message):
    query_id = message["query_id"]
    assert isinstance(query_id, str)
    assert query_id
    return query_id


def get_source(message):
    source = message["source"]
    assert isinstance(source, dict)
    assert "role" in source
    assert "id" in source
    return source


def get_payload(message):
    return message["payload"]


def get_total_sent(message):
    return message["payload"].get("total_sent")


def get_total_expected(message):
    return message["payload"].get("total_expected")


def get_count(message):
    return message["payload"]["count"]
