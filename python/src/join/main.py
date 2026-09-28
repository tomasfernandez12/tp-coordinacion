import os
import logging
import signal

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
        self.partial_tops_by_client = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        client_id, aggregation_id, partial_top = message_protocol.internal.deserialize(
            message
        )
        partial_tops = self.partial_tops_by_client.setdefault(client_id, {})
        partial_tops[aggregation_id] = partial_top

        if len(partial_tops) == AGGREGATION_AMOUNT:
            logging.info("Received tops from all aggregations for client %s", client_id)
            self.partial_tops_by_client.pop(client_id, None)
            fruit_top_by_name = {}
            for top in partial_tops.values():
                for fruit, amount in top:
                    current = fruit_top_by_name.get(fruit, fruit_item.FruitItem(fruit, 0))
                    fruit_top_by_name[fruit] = current + fruit_item.FruitItem(
                        fruit, amount
                    )

            fruit_top = sorted(fruit_top_by_name.values())[-TOP_SIZE:]
            fruit_top.reverse()
            result = [(item.fruit, item.amount) for item in fruit_top]
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, result])
            )
        ack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            for middleware_object in (self.input_queue, self.output_queue):
                try:
                    middleware_object.close()
                except Exception as exc:
                    logging.warning("Error closing middleware connection: %s", exc)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM; stopping join")
        try:
            self.input_queue.stop_consuming()
        except Exception as exc:
            logging.warning("Could not stop join consumer: %s", exc)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, join_filter.handle_sigterm)
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
