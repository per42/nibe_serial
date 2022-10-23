# NIBE Serial

This library receives and responds to messages on the NIBE MODBUS 40 RS-485 serial interface. It requests data items to be read or written, and forwards the read data.

The available data items are according to these [JSON files](https://github.com/yozik04/nibe/tree/master/nibe/data). The requests use the `name` field for identification, and the value is interpreted according to the other fields as int, float or mapping string, for example:

name                          | value
------------------------------|---------
bt1-outdoor-temperature-40004 | 11.6
hot-water-comfort-mode-47041  | ECONOMY

The program forwards UDP datagrams from NIBE, and takes data requests from MQTT. The topic is the name prefixed, and the value JSON-encoded in the payload.

The climate unit can also be configured in its logging function to continously publish a set of variables on a separate topic. See [nibe-log-set](https://github.com/per42/nibe-log-set) for configuration.


## NIBE serial interface

>With [MODBUS 40](https://partner.nibe.eu/nibedocuments/24801/031725-10.pdf), a NIBE climate unit can be controlled and monitored by external Modbus-equipped equipment.

This accessory is connected to an RS-485 bus according to the linked manual. It makes read and write requests when polled, and receives requested data. 

[NIBE ModbusManager](https://proffs.nibe.se/Proffs/For-installatoren/kommunikation/#tab2) can be used to select up to 23 data items in a LOG.SET file that, when uploaded to the climate unit from a USB drive, causes those data items to be continuously sent on the accessory interface without read requests.

> **Note**
> The serial protocol on the RS-485 bus is not [Modbus](https://en.wikipedia.org/wiki/Modbus). It is an undocumented internal NIBE protocol.

There are other devices that connect to the same interface:
- OpenHAB [Nibe Heatpump Binding](https://www.openhab.org/addons/bindings/nibeheatpump/) is an Arduino firmware or Linux service that forwards the communication to UDP. The datagrams are handled by OpenHAB or Jevgeni Kiski's [Nibe library](https://github.com/yozik04/nibe).
- [NibePi](https://github.com/anerdins/nibepi) is a RaspberryPi image with a NodeRed module for communicating, and other NodeRed modules for adjusting the climate unit to the electricity price.


## Setup

The program was developed to accept datagrams from an [Elfin EW11A](http://www.hi-flying.com/elfin-ew10-elfin-ew11) RS-485 to WiFi adapter. The adapter is configured as a UDP client sending datagrams to where this program is running according to [this configuration](EW11-udp_client.xml). It forwards raw RS-485 frames without interpretation. This program returns the response.

The serial interface seems sensitive to delays in the response. In my infrastructure replacing UDP with TCP works, but not with MQTT.

