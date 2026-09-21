import time
import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

class RabbitMQConnectionManager:

    def __init__(self, host):
        self.host = host
        self.connection = None
        self.channel = None

    def connect(self):
        if self.connection is not None and self.connection.is_open:
            return

        last_exception = None
        for _ in range(10):
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
                self.channel = self.connection.channel()
                return
            except Exception:
                pass

        raise MessageMiddlewareDisconnectedError(f"No se pudo conectar con RabbitMQ en host {self.host}") from last_exception

    def close(self):
        try:
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        finally:
            self.connection = None

    def raise_for_connection_error(self, exc):
        if isinstance(
            exc,
            (
                pika.exceptions.AMQPConnectionError,
                pika.exceptions.ConnectionClosed,
                pika.exceptions.StreamLostError,
            ),
        ):
            raise MessageMiddlewareDisconnectedError(str(exc)) from exc

        raise MessageMiddlewareMessageError(str(exc)) from exc


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.conn = RabbitMQConnectionManager(host)
        self.message_callback = None

    def start_consuming(self, on_message_callback):
        self.message_callback = on_message_callback

        try:
            self.conn.connect()
            self.conn.channel.queue_declare(queue=self.queue_name,durable=True,auto_delete=True,)
            self.conn.channel.basic_qos(prefetch_count=1)
            self.conn.channel.basic_consume(queue=self.queue_name,on_message_callback=self.on_message,auto_ack=False,)
            self.conn.channel.start_consuming()
        except Exception as exc:
            if isinstance(exc, (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError)):
                raise
            self.conn.raise_for_connection_error(exc)

    def stop_consuming(self):
        if self.conn.channel is None:
            return
        try:
            self.conn.channel.stop_consuming()
        except Exception as exc:
            raise MessageMiddlewareDisconnectedError(str(exc)) from exc

    def send(self, message):
        try:
            self.conn.connect()
            self.conn.channel.queue_declare(queue=self.queue_name,durable=True,auto_delete=True,)
            self.conn.channel.basic_publish(exchange="",routing_key=self.queue_name,body=message,)
        except Exception as exc:
            if isinstance(exc, (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError)):
                raise
            self.conn.raise_for_connection_error(exc)

    def close(self):
        try:
            self.conn.close()
        except Exception as exc:
            raise MessageMiddlewareCloseError(str(exc)) from exc

    def on_message(self, channel, method, properties, body):
        ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        try:
            self.message_callback(body, ack, nack)
        except Exception as exc:
            try:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except Exception:
                pass
            raise MessageMiddlewareMessageError(str(exc)) from exc


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self._routing_keys = routing_keys
        self.conn = RabbitMQConnectionManager(host)
        self.message_callback = None
        self.queue_name = f"{exchange_name}_queue_{time.time_ns()}"

    def start_consuming(self, on_message_callback):
        self.message_callback = on_message_callback

        try:
            self.conn.connect()
            self.conn.channel.exchange_declare(exchange=self.exchange_name,exchange_type="direct",durable=True,auto_delete=True,)
            self.conn.channel.queue_declare(queue=self.queue_name,durable=True,auto_delete=True,)

            for routing_key in self._routing_keys:
                self.conn.channel.queue_bind(exchange=self.exchange_name,queue=self.queue_name,routing_key=routing_key,)

            self.conn.channel.basic_qos(prefetch_count=1)
            self.conn.channel.basic_consume(queue=self.queue_name,on_message_callback=self.on_message,auto_ack=False,)
            self.conn.channel.start_consuming()
        except Exception as exc:
            if isinstance(exc, (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError)):
                raise
            self.conn.raise_for_connection_error(exc)

    def stop_consuming(self):
        if self.conn.channel is None:
            return
        try:
            self.conn.channel.stop_consuming()
        except Exception as exc:
            raise MessageMiddlewareDisconnectedError(str(exc)) from exc

    def send(self, message):
        try:
            self.conn.connect()
            self.conn.channel.exchange_declare(exchange=self.exchange_name,exchange_type="direct",durable=True,auto_delete=True,)
            routing_key = self._routing_keys[0] if self._routing_keys else ""
            self.conn.channel.basic_publish(exchange=self.exchange_name,routing_key=routing_key,body=message,)
        except Exception as exc:
            if isinstance(exc, (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError)):
                raise
            self.conn.raise_for_connection_error(exc)

    def close(self):
        try:
            self.conn.close()
        except Exception as exc:
            raise MessageMiddlewareCloseError(str(exc)) from exc

    def on_message(self, channel, method, properties, body):
        ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        try:
            self.message_callback(body, ack, nack)
        except Exception as exc:
            try:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except Exception:
                pass
            raise MessageMiddlewareMessageError(str(exc)) from exc
