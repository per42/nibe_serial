"""Receives and responds to messages on the Nibe MODBUS40 RS485 serial interface.

Requests data items to be read or written, and forwards the read data.

The program forwards UDP datagrams from Nibe, and takes data requests from MQTT.
"""
__version__ = "2.1.0"

from argparse import ArgumentParser
from copy import copy
from queue import Queue
from logging import Logger, getLogger, basicConfig
from socket import AF_INET, SOCK_DGRAM, socket
from typing import Callable, Mapping, Optional, Tuple, TypedDict, Union
from json import loads, dumps

from paho.mqtt.client import Client, MQTTMessage
from nibe.connection.nibegw import Response, ReadRequest, WriteRequest
from nibe.heatpump import HeatPump, Model
from construct import StreamError


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
        help="root topic; subscribes to MQTT_TOPIC/req/#, publishes on MQTT_TOPIC/req/{name}, MQTT_TOPIC/data, default=%(default)s",
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
        try:
            req: Req = {"name": msg.topic.split("/")[-1]}
            if len(msg.payload) != 0:
                req["value"] = loads(msg.payload)

            getLogger("REQ").info(req)

            req_q.put(req)
        except Exception as e:
            getLogger("REQ").warning(
                "Parsing MQTT message %s:%s failed: %s", msg.topic, msg.payload, e
            )

    mqtt = Client()
    mqtt.on_connect = on_connect
    mqtt.on_message = on_message
    mqtt.connect(args.mqtt_host, args.mqtt_port)
    mqtt.loop_start()

    def on_res(name: str, value: Value) -> None:
        getLogger("RES").info(f"{name}: {value}")

        mqtt.publish(f"{args.mqtt_topic}/res/{name}", dumps(value), 2)

        if name == "alarm-45001" and value == 251:
            req_q.put({"name": "alarm-reset-45171", "value": 1})

    def on_data(data: Mapping[str, Value]) -> None:
        getLogger("DATA").info(
            ", ".join([f"{name}: {value}" for name, value in data.items()])
        )

        mqtt.publish(f"{args.mqtt_topic}/data", dumps(data), 0)

        if "alarm-45001" in data and data["alarm-45001"] == 251:
            req_q.put({"name": "alarm-reset-45171", "value": 1})

    nibe = NibeSerial(args.nibe_model, req_q, on_res, on_data, getLogger("Nibe"))

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

    >>> req_q: Queue[Req] = Queue()
    >>> nibe = NibeSerial("f370_f470", req_q, lambda n, v: print(n, dumps(v)), lambda x: print("DATA", dumps(x)), getLogger("Nibe"))

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
    hot-water-comfort-mode-47041 "ECONOMY"

    Receive data:
    >>> nibe.handle_frame(bytes.fromhex("5c00206850ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff000018"), lambda x: print(x.hex()))
    06
    >>> nibe.handle_frame(bytes.fromhex("5c00206850449c8000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff000040"), lambda x: print(x.hex()))
    06
    DATA {"bt1-outdoor-temperature-40004": 12.8}
    >>> nibe.handle_frame(bytes.fromhex("5c00206851c9af0000449cfbff599cad004e9cf4004d9cfb0001a8d100489cc8004c9cc4000ab900004ca800004ea81400c4b70802c8b74402aba90100a3a93c002ca400002da400005ba400005c5ca40000ffff00003d"), lambda x: print(x.hex()))
    06
    DATA {"alarm-45001": 0, "bt1-outdoor-temperature-40004": -0.5, "bt20-exhaust-air-temp-1-40025": 17.3, "bt6-hw-load-40014": 24.4, "bt7-hw-top-40013": 25.1, "calc-supply-s1-43009": 20.9, "bt2-supply-temp-s1-40008": 20.0, "eb100-ep14-bt3-return-temp-40012": 19.6, "allow-additive-heating-47370": "OFF", "int-el-add-power-43084": 0.0, "prio-43086": "HOT WATER", "start-temperature-hw-normal-47044": 52.0, "stop-temperature-hw-normal-47048": 58.0, "cpr-status-ep14-43435": "ON", "compressor-state-ep14-43427": "RUNNING", "aa23-be5-energy-1-42028": 0.0, "aa23-be5-eme20-total-energy-42075": 0.0}
    """

    def __init__(
        self,
        model: str,
        req_q: Queue[Req],
        on_res: Callable[[str, Value], None],
        on_data: Callable[[Mapping[str, Value]], None],
        logger: Logger,
    ) -> None:
        self._logger = logger
        self._req_q = req_q
        self._on_res = on_res
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
                        self._on_res(
                            *self._decode_raw_data(
                                fields.data.coil_address, fields.data.value
                            )
                        )

                    elif fields.cmd == "MODBUS_WRITE_RESP":
                        self._logger.getChild("DATA").info(
                            f"Write {'succeded' if fields.data.result else 'failed'}"
                        )

                    elif fields.cmd == "MODBUS_DATA_MSG":
                        di = iter(fields.data)
                        data = {}

                        for d in di:
                            if d.coil_address != 0xFFFF:
                                try:
                                    k, v = self._decode_raw_data(
                                        d.coil_address, d.value
                                    )
                                except StreamError:
                                    d2 = next(di)
                                    assert d2.coil_address == d.coil_address + 1
                                    k, v = self._decode_raw_data(
                                        d.coil_address, d.value + d2.value
                                    )
                                data[k] = v

                        if len(data) >= 1:
                            self._on_data(data)

            except Exception as e:
                self._logger.warning(f"{e}: {payload.hex()}")

    def _decode_raw_data(self, address: int, value: bytes) -> Tuple[str, Value]:
        coil = copy(self._heatpump.get_coil_by_address(address))
        coil.raw_value = value

        return coil.name, coil.value


if __name__ == "__main__":
    main()
