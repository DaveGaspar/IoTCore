import serial
import time
import logging
import json
from datetime import datetime
from dataclasses import dataclass
from typing import Optional

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# ========== Logging Setup ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('psm5000_publisher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== Data Class ==========
@dataclass
class PMData:
    pm1_0: float
    pm2_5: float
    pm10: float
    timestamp: float

    def to_json(self) -> str:
        """Convert to JSON string with local timestamp including timezone"""
        local_time = datetime.fromtimestamp(self.timestamp).astimezone()
        tz_offset = local_time.strftime('%z')  # e.g., +0300
        # Format it as +03:00
        tz_formatted = f"{tz_offset[:3]}:{tz_offset[3:]}"
        formatted_time = local_time.strftime('%d-%m-%Y %H:%M:%S') + f" {tz_formatted}"

        data = {
            "pm1_0": self.pm1_0,
            "pm2_5": self.pm2_5,
            "pm10": self.pm10,
            "timestamp": formatted_time
        }
        return json.dumps(data, indent=2)

# ========== Sensor Reader ==========
class PSM5000Reader:
    HEADER = b'\x42\x4D'
    FRAME_LENGTH = 32
    BAUD_RATE = 9600
    DEFAULT_UART_PORT = '/dev/ttyS0'

    def __init__(self, port: str = DEFAULT_UART_PORT, timeout: float = 1.0):
        self.port = port
        self.timeout = timeout
        self.serial_conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        try:
            self.serial_conn = serial.Serial(
                port=self.port,
                baudrate=self.BAUD_RATE,
                timeout=self.timeout,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                bytesize=serial.EIGHTBITS
            )
            if not self.serial_conn.is_open:
                self.serial_conn.open()
            logger.info(f"Connected to PSM5000 on {self.port}")
        except serial.SerialException as e:
            logger.error(f"Failed to connect: {e}")
            raise

    def disconnect(self):
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            logger.info("Disconnected from PSM5000")

    def _validate_checksum(self, data: bytes) -> bool:
        if len(data) != self.FRAME_LENGTH:
            return False
        calculated_sum = sum(data[:-2])
        received_checksum = int.from_bytes(data[-2:], byteorder='big')
        return calculated_sum == received_checksum

    def read_raw_data(self) -> Optional[bytes]:
        if not self.serial_conn or not self.serial_conn.is_open:
            logger.error("Serial connection not established")
            return None
        try:
            header = self.serial_conn.read(2)
            if len(header) != 2 or header != self.HEADER:
                logger.debug("Invalid header received")
                return None
            remaining = self.serial_conn.read(self.FRAME_LENGTH - 2)
            if len(remaining) != self.FRAME_LENGTH - 2:
                logger.debug("Incomplete frame received")
                return None
            frame = header + remaining
            return frame if self._validate_checksum(frame) else None
        except serial.SerialException as e:
            logger.error(f"Serial error: {e}")
            return None

    def parse_data(self, raw_data: bytes) -> Optional[PMData]:
        if not raw_data or len(raw_data) != self.FRAME_LENGTH:
            return None
        try:
            pm1_0 = int.from_bytes(raw_data[4:6], byteorder='big') / 10.0
            pm2_5 = int.from_bytes(raw_data[6:8], byteorder='big') / 10.0
            pm10 = int.from_bytes(raw_data[8:10], byteorder='big') / 10.0
            return PMData(pm1_0=pm1_0, pm2_5=pm2_5, pm10=pm10, timestamp=time.time())
        except Exception as e:
            logger.error(f"Parsing error: {e}")
            return None

    def read_pm_data(self) -> Optional[PMData]:
        raw = self.read_raw_data()
        return self.parse_data(raw) if raw else None

# ========== MQTT Publisher ==========
def publish_to_aws_iot(payload: str, topic: str):
    client = AWSIoTMQTTClient("DavitRaspberry")
    client.configureEndpoint("d09530762c0nb5dceftxl-ats.iot.us-east-1.amazonaws.com", 8883)
    client.configureCredentials("./AmazonRootCA1.pem", "./private.pem.key", "./certificate.pem.crt")

    try:
        client.connect()
        logger.info("Connected to AWS IoT Core")
        client.publish(topic, payload, 0)
        logger.info(f"Published message to topic '{topic}': {payload}")
    except Exception as e:
        logger.error(f"MQTT publish failed: {e}")
    finally:
        client.disconnect()
        logger.info("Disconnected from AWS IoT")

# ========== Main ==========
def main():
    topic = "Davit/pms5003/data"
    try:
        with PSM5000Reader() as reader:
            pm_data = reader.read_pm_data()
            if pm_data:
                json_payload = pm_data.to_json()
                print(json_payload)  # Print human-readable JSON
                publish_to_aws_iot(json_payload, topic)
            else:
                logger.warning("No PM data read from sensor")
    except Exception as e:
        logger.error(f"Main error: {e}")

if __name__ == "__main__":
    main()

