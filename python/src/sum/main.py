import os
import time
import signal
import zlib
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

assert ID >= 0
assert SUM_AMOUNT > 0
assert AGGREGATION_AMOUNT > 0
assert ID < SUM_AMOUNT
assert MOM_HOST
assert INPUT_QUEUE
assert SUM_PREFIX
assert AGGREGATION_PREFIX

_CLOSED_QUERY_TTL_SECONDS = 300
_MAX_RECHECK_ROUNDS = 20
_CLEANUP_INTERVAL_SECONDS = 60
_CLOSE_EXCHANGE = f"{SUM_PREFIX}_close_exchange"
_COUNT_EXCHANGE = f"{SUM_PREFIX}_count_exchange"


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.input_queue.register_extra_consumer(
            f"{SUM_PREFIX}_close_{ID}",
            self.process_close_control_message,
            exchange_name=_CLOSE_EXCHANGE,
        )
        self.input_queue.register_extra_consumer(
            f"{SUM_PREFIX}_count_{ID}",
            self.process_count_reply,
            exchange_name=_COUNT_EXCHANGE,
        )

        self.close_broadcaster = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            _CLOSE_EXCHANGE,
            [],
            exchange_type="fanout",
        )

        self.count_reply_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            _COUNT_EXCHANGE,
            [],
        )

        self.data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            AGGREGATION_PREFIX,
            [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)],
        )

        self.amount_by_fruit_by_query = {}
        self.data_count_by_query = {}
        self.closed_queries = {}
        self.coordination = {}
        self._last_cleanup = time.monotonic()

    def _maybe_cleanup(self):
        now = time.monotonic()
        if now - self._last_cleanup < _CLEANUP_INTERVAL_SECONDS:
            return
        expired = [
            qid
            for qid, closed_at in self.closed_queries.items()
            if now - closed_at > _CLOSED_QUERY_TTL_SECONDS
        ]
        for qid in expired:
            self.closed_queries.pop(qid, None)
        self._last_cleanup = now

    def _get_query_state(self, query_id):
        if query_id not in self.amount_by_fruit_by_query:
            self.amount_by_fruit_by_query[query_id] = {}
        return self.amount_by_fruit_by_query[query_id]

    def _get_aggregation_index(self, fruit):
        return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

    def _process_data(self, query_id, fruit, amount):
        if query_id in self.closed_queries:
            logging.debug("Ignoring late data for closed query %s", query_id)
            return

        query_state = self._get_query_state(query_id)
        query_state[fruit] = query_state.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        self.data_count_by_query[query_id] = (
            self.data_count_by_query.get(query_id, 0) + 1
        )

    def _emit_query_results(self, query_id):
        logging.info("Emitting local sum results for query %s", query_id)

        query_state = dict(self.amount_by_fruit_by_query.get(query_id, {}))
        self.amount_by_fruit_by_query.pop(query_id, None)

        for final_fruit_item in query_state.values():
            agg_index = self._get_aggregation_index(final_fruit_item.fruit)
            message = message_protocol.internal.build_message(
                message_protocol.internal.TYPE_DATA,
                query_id,
                message_protocol.internal.ROLE_SUM,
                ID,
                {
                    "fruit": final_fruit_item.fruit,
                    "amount": final_fruit_item.amount,
                },
            )
            self.data_output_exchange.send(
                message_protocol.internal.serialize(message),
                routing_key=f"{AGGREGATION_PREFIX}_{agg_index}",
            )

    def _emit_eof_to_aggregations(self, query_id):
        logging.info("Emitting EOF to aggregations for query %s", query_id)

        eof_message = message_protocol.internal.build_eof_message(
            query_id,
            message_protocol.internal.ROLE_SUM,
            ID,
        )
        serialized_eof = message_protocol.internal.serialize(eof_message)
        for i in range(AGGREGATION_AMOUNT):
            self.data_output_exchange.send(
                serialized_eof,
                routing_key=f"{AGGREGATION_PREFIX}_{i}",
            )

    def _send_count_reply(self, query_id, initiator_id):
        count = self.data_count_by_query.get(query_id, 0)
        reply = message_protocol.internal.build_count_reply_message(
            query_id, ID, count,
        )
        self.count_reply_exchange.send(
            message_protocol.internal.serialize(reply),
            routing_key=f"{SUM_PREFIX}_count_{initiator_id}",
        )

    def _broadcast_close_signal(self, query_id, total_expected):
        msg = message_protocol.internal.build_close_signal_message(
            query_id, ID, total_expected,
        )
        self.close_broadcaster.send(message_protocol.internal.serialize(msg))

    def _broadcast_recheck_signal(self, query_id):
        msg = message_protocol.internal.build_recheck_signal_message(query_id, ID)
        self.close_broadcaster.send(message_protocol.internal.serialize(msg))

    def _broadcast_go_signal(self, query_id):
        msg = message_protocol.internal.build_go_signal_message(query_id, ID)
        self.close_broadcaster.send(message_protocol.internal.serialize(msg))

    def _process_gateway_eof(self, internal_message):
        query_id = message_protocol.internal.get_query_id(internal_message)

        if query_id in self.closed_queries:
            return

        total_sent = message_protocol.internal.get_total_sent(internal_message)

        logging.info(
            "Processing gateway EOF for query %s (total_sent=%s), starting coordination",
            query_id,
            total_sent,
        )

        if query_id not in self.coordination:
            self.coordination[query_id] = {
                "total_expected": total_sent,
                "initiator_id": ID,
                "replies": {},
                "recheck_rounds": 0,
            }

        self._broadcast_close_signal(query_id, total_sent)

    def process_close_control_message(self, message, ack, nack):
        try:
            internal_message = message_protocol.internal.deserialize(message)
            query_id = message_protocol.internal.get_query_id(internal_message)
            source = message_protocol.internal.get_source(internal_message)

            if message_protocol.internal.is_close_signal_message(internal_message):
                if query_id in self.closed_queries:
                    ack()
                    return

                initiator_id = source["id"]
                total_expected = message_protocol.internal.get_total_expected(
                    internal_message
                )

                logging.info(
                    "Close signal for query %s from initiator %d (total_expected=%s)",
                    query_id,
                    initiator_id,
                    total_expected,
                )

                if query_id not in self.coordination:
                    self.coordination[query_id] = {
                        "total_expected": total_expected,
                        "initiator_id": initiator_id,
                        "replies": {},
                        "recheck_rounds": 0,
                    }

                self._send_count_reply(query_id, initiator_id)

            elif message_protocol.internal.is_recheck_signal_message(internal_message):
                if query_id in self.closed_queries:
                    ack()
                    return

                if query_id not in self.coordination:
                    logging.warning(
                        "Recheck for unknown query %s, ignoring", query_id
                    )
                    ack()
                    return

                initiator_id = self.coordination[query_id]["initiator_id"]
                logging.info("Recheck signal for query %s", query_id)
                self._send_count_reply(query_id, initiator_id)

            elif message_protocol.internal.is_go_signal_message(internal_message):
                if query_id in self.closed_queries:
                    ack()
                    return

                logging.info(
                    "Go signal for query %s, emitting results", query_id
                )

                self._emit_query_results(query_id)
                self._emit_eof_to_aggregations(query_id)

                self.closed_queries[query_id] = time.monotonic()
                self.coordination.pop(query_id, None)
                self.data_count_by_query.pop(query_id, None)

            else:
                logging.error("Unsupported control message type in sum")
                nack()
                return

            ack()
        except Exception as exc:
            logging.error(exc)
            nack()

    def process_count_reply(self, message, ack, nack):
        try:
            internal_message = message_protocol.internal.deserialize(message)
            query_id = message_protocol.internal.get_query_id(internal_message)
            source = message_protocol.internal.get_source(internal_message)
            sum_id = source["id"]
            count = message_protocol.internal.get_count(internal_message)

            if query_id in self.closed_queries:
                ack()
                return

            if query_id not in self.coordination:
                ack()
                return

            coord = self.coordination[query_id]

            if coord["initiator_id"] != ID:
                ack()
                return

            coord["replies"][sum_id] = count

            logging.info(
                "Count reply for query %s: sum_%d=%d (%d/%d collected)",
                query_id,
                sum_id,
                count,
                len(coord["replies"]),
                SUM_AMOUNT,
            )

            if len(coord["replies"]) < SUM_AMOUNT:
                ack()
                return

            total_processed = sum(coord["replies"].values())
            total_expected = coord["total_expected"]

            if total_expected is None or total_processed == total_expected:
                logging.info(
                    "All data accounted for query %s (%d/%s), broadcasting GO",
                    query_id,
                    total_processed,
                    total_expected,
                )
                self._broadcast_go_signal(query_id)
            else:
                coord["recheck_rounds"] += 1

                if coord["recheck_rounds"] >= _MAX_RECHECK_ROUNDS:
                    logging.error(
                        "Max recheck rounds for query %s (processed=%d, expected=%s), aborting",
                        query_id,
                        total_processed,
                        total_expected,
                    )
                    self.coordination.pop(query_id, None)
                    ack()
                    return

                logging.info(
                    "Count mismatch for query %s (%d/%s), RECHECK round %d",
                    query_id,
                    total_processed,
                    total_expected,
                    coord["recheck_rounds"],
                )
                coord["replies"] = {}
                self._broadcast_recheck_signal(query_id)

            ack()
        except Exception as exc:
            logging.error(exc)
            nack()

    def process_data_messsage(self, message, ack, nack):
        try:
            self._maybe_cleanup()

            internal_message = message_protocol.internal.deserialize(message)
            source = message_protocol.internal.get_source(internal_message)

            assert source["role"] in {
                message_protocol.internal.ROLE_GATEWAY,
                message_protocol.internal.ROLE_SUM,
                message_protocol.internal.ROLE_AGGREGATION,
                message_protocol.internal.ROLE_JOIN,
            }

            if message_protocol.internal.is_data_message(internal_message):
                payload = message_protocol.internal.get_payload(internal_message)
                assert isinstance(payload, dict)
                assert "fruit" in payload
                assert "amount" in payload

                self._process_data(
                    message_protocol.internal.get_query_id(internal_message),
                    payload["fruit"],
                    payload["amount"],
                )
            elif message_protocol.internal.is_eof_message(internal_message):
                self._process_gateway_eof(internal_message)
            else:
                logging.error("Unsupported message type received in sum")
                nack()
                return

            ack()
        except Exception as exc:
            logging.error(exc)
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

    def request_shutdown(self):
        try:
            self.input_queue.stop_consuming()
        except Exception as exc:
            logging.error(exc)

    def close(self):
        close_errors = []

        for resource in (
            self.input_queue,
            self.close_broadcaster,
            self.count_reply_exchange,
            self.data_output_exchange,
        ):
            try:
                resource.close()
            except Exception as exc:
                logging.error(exc)
                close_errors.append(exc)

        if close_errors:
            raise close_errors[0]


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def _handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        sum_filter.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        sum_filter.start()
    finally:
        try:
            sum_filter.close()
        except Exception as exc:
            logging.error(exc)

    return 0


if __name__ == "__main__":
    main()
