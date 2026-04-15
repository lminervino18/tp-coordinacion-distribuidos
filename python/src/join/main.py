import os
import signal
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

assert SUM_AMOUNT > 0
assert AGGREGATION_AMOUNT > 0
assert TOP_SIZE > 0
assert MOM_HOST
assert INPUT_QUEUE
assert OUTPUT_QUEUE
assert SUM_PREFIX
assert AGGREGATION_PREFIX


class JoinFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops_by_query = {}

    def _get_query_partials(self, query_id):
        assert isinstance(query_id, str)
        assert query_id

        if query_id not in self.partial_tops_by_query:
            self.partial_tops_by_query[query_id] = {}
        return self.partial_tops_by_query[query_id]

    def _build_final_top(self, partial_top_by_aggregation):
        assert isinstance(partial_top_by_aggregation, dict)
        assert len(partial_top_by_aggregation) == AGGREGATION_AMOUNT

        merged_by_fruit = {}

        for partial_top in partial_top_by_aggregation.values():
            assert isinstance(partial_top, list)

            for current_fruit, current_amount in partial_top:
                assert isinstance(current_fruit, str)
                assert current_fruit
                assert isinstance(current_amount, int)
                assert current_amount >= 0

                current_item = fruit_item.FruitItem(current_fruit, current_amount)
                merged_by_fruit[current_fruit] = merged_by_fruit.get(
                    current_fruit,
                    fruit_item.FruitItem(current_fruit, 0),
                ) + current_item

        final_top_items = sorted(merged_by_fruit.values(), reverse=True)[:TOP_SIZE]
        assert len(final_top_items) <= TOP_SIZE

        return [
            (current_item.fruit, current_item.amount) for current_item in final_top_items
        ]

    def _process_partial_top(self, query_id, source_id, partial_top):
        assert isinstance(query_id, str)
        assert query_id
        assert isinstance(source_id, int)
        assert 0 <= source_id < AGGREGATION_AMOUNT
        assert isinstance(partial_top, list)

        logging.info(
            "Processing partial top for query %s: aggregation %d/%d",
            query_id,
            len(self._get_query_partials(query_id)) + 1,
            AGGREGATION_AMOUNT,
        )

        partials = self._get_query_partials(query_id)
        partials[source_id] = partial_top

        assert len(partials) <= AGGREGATION_AMOUNT

        if len(partials) < AGGREGATION_AMOUNT:
            return

        final_top = self._build_final_top(partials)
        output_message = message_protocol.internal.build_final_top_message(
            query_id,
            final_top,
        )
        self.output_queue.send(message_protocol.internal.serialize(output_message))
        self.partial_tops_by_query.pop(query_id, None)

    def process_messsage(self, message, ack, nack):
        try:
            internal_message = message_protocol.internal.deserialize(message)
            source = message_protocol.internal.get_source(internal_message)

            assert source["role"] in {
                message_protocol.internal.ROLE_GATEWAY,
                message_protocol.internal.ROLE_SUM,
                message_protocol.internal.ROLE_AGGREGATION,
                message_protocol.internal.ROLE_JOIN,
            }

            if not message_protocol.internal.is_partial_top_message(internal_message):
                logging.error("Unsupported message type received in join")
                nack()
                return

            self._process_partial_top(
                message_protocol.internal.get_query_id(internal_message),
                source["id"],
                message_protocol.internal.get_payload(internal_message),
            )
            ack()
        except Exception as exc:
            logging.error(exc)
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def request_shutdown(self):
        try:
            self.input_queue.stop_consuming()
        except Exception as exc:
            logging.error(exc)

    def close(self):
        close_errors = []

        try:
            self.input_queue.close()
        except Exception as exc:
            logging.error(exc)
            close_errors.append(exc)

        try:
            self.output_queue.close()
        except Exception as exc:
            logging.error(exc)
            close_errors.append(exc)

        if close_errors:
            raise close_errors[0]


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def _handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        join_filter.request_shutdown()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        join_filter.start()
    finally:
        try:
            join_filter.close()
        except Exception as exc:
            logging.error(exc)

    return 0


if __name__ == "__main__":
    main()