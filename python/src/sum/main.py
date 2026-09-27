import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control_exchange"
SUM_CONTROL_ROUTING_KEY = f"{SUM_PREFIX}_eof"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # Se usa la misma routing key para que cada sum suscripto reciba una copia del EOF.
        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_ROUTING_KEY]
        )
        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_ROUTING_KEY]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client = {}
        # Ambos consumers acceden al acumulado desde threads distintos; este lock serializa sus cambios.
        self.closed_clients = set()
        self.state_lock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        amount_by_fruit = self.amount_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _aggregation_index(self, fruit):
        fruit_hash = hashlib.sha256(fruit.encode("utf-8")).digest()
        return int.from_bytes(fruit_hash, "big") % AGGREGATION_AMOUNT

    def _process_eof(self, client_id):
        if client_id in self.closed_clients:
            return

        logging.info("Broadcasting data messages for client %s", client_id)
        amount_by_fruit = self.amount_by_client.pop(client_id, {})
        for final_fruit_item in amount_by_fruit.values():
            aggregation_index = self._aggregation_index(final_fruit_item.fruit)
            self.data_output_exchanges[aggregation_index].send(
                message_protocol.internal.serialize(
                    [
                        client_id,
                        final_fruit_item.fruit,
                        final_fruit_item.amount,
                    ]
                )
            )

        logging.info("Broadcasting EOF message for client %s", client_id)
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize([client_id, ID])
            )
        self.closed_clients.add(client_id)

    def process_control_message(self, message, ack, nack):
        [client_id] = message_protocol.internal.deserialize(message)
        with self.state_lock:
            self._process_eof(client_id)
        ack()

    def process_data_messsage(self, message, ack, nack):
        with self.state_lock:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                self._process_data(*fields)
                ack()
                return
        if len(fields) == 1:
            self.control_output_exchange.send(
                message_protocol.internal.serialize(fields)
            )
        else:
            raise ValueError(f"Unexpected sum input message: {fields}")
        ack()

    def start(self):
        control_thread = threading.Thread(
            target=self.control_input_exchange.start_consuming,
            args=(self.process_control_message,),
            name=f"sum-control-{ID}",
            daemon=True,
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
