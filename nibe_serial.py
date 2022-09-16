"""Receives and responds to messages on the Nibe MODBUS40 RS485 serial interface.

Requests data items to be read or written, and forwards the read data.

The program forwards UDP datagrams from Nibe, and takes data requests from MQTT.
"""
__version__ = "1.0.0"

from argparse import ArgumentParser
from copy import copy
from queue import Queue
from logging import Logger, getLogger, basicConfig
from socket import AF_INET, SOCK_DGRAM, socket
from typing import Callable, Optional, TypedDict, Union
from json import loads, dumps

from paho.mqtt.client import Client, MQTTMessage
from nibe.connection.nibegw import Response, ReadRequest, WriteRequest
from nibe.heatpump import HeatPump, Model


def main() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--mqtt-host", default="127.0.0.1", help="default=%(default)s")
    parser.add_argument(
        "--mqtt-port",
        type=int,
        default=1883,
        help="default=%(default)s",
    )
    parser.add_argument(
        "--mqtt-topic",
        default="nibe",
        help="root topic; subscribes to MQTT_TOPIC/req/#, publishes on MQTT_TOPIC/req/{name}, default=%(default)s",
    )
    parser.add_argument(
        "--nibe-port",
        type=int,
        default=9999,
        help="Network port listening for nibe UDP packets, default=%(default)s",
    )
    parser.add_argument(
        "--nibe-model",
        default="f370_f470",
        choices=[m.value for m in Model.__members__.values()],
        help="default=%(default)s",
    )
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args()

    basicConfig(level=args.log_level)

    req_q: Queue[Req] = Queue()
    req_q.put({"name": "alarm-45001"})

    def on_connect(client: Client, userdata, flags, rc) -> None:
        client.subscribe(f"{args.mqtt_topic}/req/#", 2)

    def on_message(client: Client, userdata, msg: MQTTMessage) -> None:
        req: Req = {"name": msg.topic.split("/")[-1]}
        if len(msg.payload) != 0:
            req["value"] = loads(msg.payload)

        getLogger("REQ").info(req)

        req_q.put(req)

    mqtt = Client()
    mqtt.on_connect = on_connect
    mqtt.on_message = on_message
    mqtt.connect(args.mqtt_host, args.mqtt_port)
    mqtt.loop_start()

    def on_data(name: str, value: Value) -> None:
        getLogger("DATA").info(f"{name}: {value}")

        mqtt.publish(f"{args.mqtt_topic}/res/{name}", dumps(value), 2)

        if name == "alarm-45001" and value == 251:
            req_q.put({"name": "alarm-reset-45171", "value": 1})

    nibe = NibeSerial(args.nibe_model, req_q, on_data, getLogger("Nibe"))

    udp = Udp(args.nibe_port)

    while True:
        nibe.handle_frame(udp.recv(), udp.send)


class Udp:
    """Receives a datagram on bound port, and replies to the source"""

    def __init__(self, port: int) -> None:
        self._sock = socket(AF_INET, SOCK_DGRAM)
        self._sock.bind(("", port))

    def recv(self) -> bytes:
        data, self._addr = self._sock.recvfrom(512)
        getLogger("IN").debug(data.hex())
        return data

    def send(self, data: bytes) -> None:
        getLogger("OUT").debug(data.hex())
        self._sock.sendto(data, self._addr)


Value = Union[int, float, str]


class ReadReq(TypedDict):
    name: str


class Req(ReadReq, total=False):
    value: Value


class NibeSerial:
    """Handles messages intended for MODBUS40.

    It forwards read and write requests from a queue. Read data is sent to a callback

    >>> req_q: Queue[dict] = Queue()
    >>> nibe = NibeSerial("f370_f470", req_q, lambda *x: print(*x), getLogger("Nibe"))

    ACK read poll:

    >>> nibe.handle_frame(bytes.fromhex("5c0020690049"), lambda x: print(x.hex()))
    06

    Write on write poll:

    >>> req_q.put({"name": "hot-water-comfort-mode-47041", "value": "ECONOMY"})
    >>> req_q.put({"name": "alarm-reset-45171", "value": 1})
    >>> nibe.handle_frame(bytes.fromhex("5c00206b004b"), lambda x: print(x.hex()))
    c06b06c1b700000000db
    >>> nibe.handle_frame(bytes.fromhex("06"), lambda x: print(x.hex()))
    >>> nibe.handle_frame(bytes.fromhex("5c00206b004b"), lambda x: print(x.hex()))
    c06b0673b0010000006f
    >>> nibe.handle_frame(bytes.fromhex("06"), lambda x: print(x.hex()))

    Read on read poll:

    >>> req_q.put({"name": "hot-water-comfort-mode-47041"})
    >>> nibe.handle_frame(bytes.fromhex("5c0020690049"), lambda x: print(x.hex()))
    c06902c1b7dd
    >>> nibe.handle_frame(bytes.fromhex("06"), lambda x: print(x.hex()))
    >>> nibe.handle_frame(bytes.fromhex("5c00206a06c1b7000044027c"), lambda x: print(x.hex()))
    06
    hot-water-comfort-mode-47041 ECONOMY
    """

    def __init__(
        self,
        model: str,
        req_q: Queue[Req],
        on_data: Callable[[str, Value], None],
        logger: Logger,
    ) -> None:
        self._logger = logger
        self._req_q = req_q
        self._on_data = on_data

        self._heatpump = HeatPump(Model(model))
        self._heatpump.initialize()

        self._op: Optional[dict] = None

    def handle_frame(self, payload: bytes, respond: Callable[[bytes], None]) -> None:
        if payload[0] in [0x06, 0x15]:
            if self._op is not None and self._op["pending"]:
                if payload[0] == 0x06:
                    self._op = None
                    self._req_q.task_done()
                else:
                    self._op["pending"] = False

            payload = payload[1:]

        if len(payload) != 0:
            try:
                fields = Response.parse(payload).fields.value

                if fields["address"] == "MODBUS40":
                    if self._op == None and not self._req_q.empty():
                        req = self._req_q.get_nowait()
                        coil = copy(self._heatpump.get_coil_by_name(req["name"]))
                        if "value" in req:
                            coil.value = req["value"]
                        self._op = {"coil": coil, "pending": False}

                    if (
                        fields.cmd in {"MODBUS_READ_REQ", "MODBUS_WRITE_REQ"}
                        and self._op is not None
                    ):

                        if (
                            fields.cmd == "MODBUS_READ_REQ"
                            and self._op["coil"].value is None
                        ):
                            m = ReadRequest.build(
                                dict(
                                    fields=dict(
                                        value=dict(
                                            coil_address=self._op["coil"].address
                                        )
                                    )
                                )
                            )

                            if self._op["pending"]:
                                self._logger.warning("Resending unacked")
                            else:
                                self._op["pending"] = True

                            respond(m)

                        elif (
                            fields.cmd == "MODBUS_WRITE_REQ"
                            and self._op["coil"].value is not None
                        ):
                            m = WriteRequest.build(
                                dict(
                                    fields=dict(
                                        value=dict(
                                            coil_address=self._op["coil"].address,
                                            value=self._op["coil"].raw_value,
                                        )
                                    )
                                )
                            )

                            if self._op["pending"]:
                                self._logger.warning("Resending unacked")
                            else:
                                self._op["pending"] = True

                            respond(m)

                        else:
                            respond(bytes.fromhex("06"))

                    else:
                        respond(bytes.fromhex("06"))

                    if fields.cmd == "MODBUS_READ_RESP":
                        self._on_raw_data(fields.data.coil_address, fields.data.value)

                    elif fields.cmd == "MODBUS_WRITE_RESP":
                        self._logger.getChild("DATA").info(
                            f"Write {'succeded' if fields.data.result else 'failed'}"
                        )

                    elif fields.cmd == "MODBUS_DATA_MSG":
                        for d in fields.data:
                            if d.coil_address != 0xFFFF:
                                self._on_raw_data(d.coil_address, d.value)

            except Exception as e:
                self._logger.warning(e)

    def _on_raw_data(self, address: int, value: bytes) -> None:
        coil = copy(self._heatpump.get_coil_by_address(address))
        coil.raw_value = value

        self._on_data(coil.name, coil.value)


if __name__ == "__main__":
    main()