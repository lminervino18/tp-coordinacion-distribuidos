import os
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


class JoinFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def _build_final_top(self, partial_top):
        fruit_items = [
            fruit_item.FruitItem(current_fruit, current_amount)
            for current_fruit, current_amount in partial_top
        ]
        fruit_items.sort(reverse=True)
        final_top_items = fruit_items[:TOP_SIZE]
        return [
            (current_item.fruit, current_item.amount) for current_item in final_top_items
        ]

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Processing message")
            internal_message = message_protocol.internal.deserialize(message)

            if not message_protocol.internal.is_partial_top_message(internal_message):
                logging.error("Unsupported message type received in join")
                nack()
                return

            query_id = message_protocol.internal.get_query_id(internal_message)
            partial_top = message_protocol.internal.get_payload(internal_message)
            final_top = self._build_final_top(partial_top)

            output_message = message_protocol.internal.build_final_top_message(
                query_id,
                final_top,
            )
            self.output_queue.send(message_protocol.internal.serialize(output_message))
            ack()
        except Exception as exc:
            logging.error(exc)
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()