import serial
import time
import logging
from dataclasses import dataclass
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('psm5000_reader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class PMData:
    """Data class to hold particulate matter measurements"""
    pm1_0: float
    pm2_5: float
    pm10: float
    timestamp: float

class PSM5000Reader:
    """
    Class to handle communication with PSM5000 PM2.5 sensor via UART
    """
    
    # PSM5000 protocol constants
    HEADER = b'\x42\x4D'  # 'BM' in ASCII
    FRAME_LENGTH = 32
    BAUD_RATE = 9600
    DEFAULT_UART_PORT = '/dev/ttyS0'  # Default UART on Raspberry Pi 3B+
    
    def __init__(self, port: str = DEFAULT_UART_PORT, timeout: float = 1.0):
        """
        Initialize the PSM5000 reader
        
        Args:
            port: UART port address
            timeout: Serial communication timeout in seconds
        """
        self.port = port
        self.timeout = timeout
        self.serial_conn = None
        
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        
    def connect(self) -> None:
        """Establish serial connection to the sensor"""
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
            logger.error(f"Failed to connect to PSM5000: {e}")
            raise
            
    def disconnect(self) -> None:
        """Close the serial connection"""
        if self.serial_conn and self.serial_conn.is_open:
            self.serial_conn.close()
            logger.info("Disconnected from PSM5000")
            
    def _validate_checksum(self, data: bytes) -> bool:
        """
        Validate the checksum of the received data frame
        
        Args:
            data: Complete frame including headers and checksum
            
        Returns:
            bool: True if checksum is valid, False otherwise
        """
        if len(data) != self.FRAME_LENGTH:
            return False
            
        # Sum of all bytes except last 2 (checksum bytes)
        calculated_sum = sum(data[:-2])
        
        # The checksum is the last 2 bytes (big-endian)
        received_checksum = int.from_bytes(data[-2:], byteorder='big')
        
        return calculated_sum == received_checksum
    
    def read_raw_data(self) -> Optional[bytes]:
        """
        Read a complete data frame from the sensor
        
        Returns:
            bytes: Complete frame if successfully read, None otherwise
        """
        if not self.serial_conn or not self.serial_conn.is_open:
            logger.error("Serial connection not established")
            return None
            
        try:
            # Wait for header bytes
            header = self.serial_conn.read(2)
            if len(header) != 2 or header != self.HEADER:
                logger.debug("Invalid or incomplete header received")
                return None
                
            # Read the rest of the frame (FRAME_LENGTH - 2 bytes)
            remaining_bytes = self.serial_conn.read(self.FRAME_LENGTH - 2)
            if len(remaining_bytes) != self.FRAME_LENGTH - 2:
                logger.debug("Incomplete frame received")
                return None
                
            # Combine header and remaining bytes
            full_frame = header + remaining_bytes
            
            # Validate checksum
            if not self._validate_checksum(full_frame):
                logger.warning("Checksum validation failed")
                return None
                
            return full_frame
            
        except serial.SerialException as e:
            logger.error(f"Serial communication error: {e}")
            return None
            
    def parse_data(self, raw_data: bytes) -> Optional[PMData]:
        """
        Parse raw data bytes into PMData object
        
        Args:
            raw_data: Complete validated data frame
            
        Returns:
            PMData: Parsed data if successful, None otherwise
        """
        if not raw_data or len(raw_data) != self.FRAME_LENGTH:
            return None
            
        try:
            # Extract PM values (concentration in μg/m³)
            pm1_0 = int.from_bytes(raw_data[4:6], byteorder='big') / 10.0
            pm2_5 = int.from_bytes(raw_data[6:8], byteorder='big') / 10.0
            pm10 = int.from_bytes(raw_data[8:10], byteorder='big') / 10.0
            
            return PMData(
                pm1_0=pm1_0,
                pm2_5=pm2_5,
                pm10=pm10,
                timestamp=time.time()
            )
            
        except Exception as e:
            logger.error(f"Error parsing data: {e}")
            return None
            
    def read_pm_data(self) -> Optional[PMData]:
        """
        Read and parse PM data from the sensor
        
        Returns:
            PMData: Latest PM measurements if successful, None otherwise
        """
        raw_data = self.read_raw_data()
        if raw_data:
            return self.parse_data(raw_data)
        return None
        
    def continuous_read(self, interval: float = 1.0, max_reads: Optional[int] = None):
        """
        Continuously read data from the sensor
        
        Args:
            interval: Time between reads in seconds
            max_reads: Maximum number of reads (None for infinite)
        """
        count = 0
        try:
            while max_reads is None or count < max_reads:
                pm_data = self.read_pm_data()
                if pm_data:
                    logger.info(
                        f"PM1.0: {pm_data.pm1_0} μg/m³, "
                        f"PM2.5: {pm_data.pm2_5} μg/m³, "
                        f"PM10: {pm_data.pm10} μg/m³"
                    )
                else:
                    logger.warning("Failed to read valid data")
                    
                count += 1
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Continuous reading stopped by user")
            
def main():
    """Main function to demonstrate usage"""
    try:
        with PSM5000Reader() as reader:
            # Test single read
            pm_data = reader.read_pm_data()
            if pm_data:
                print(f"Current PM2.5: {pm_data.pm2_5} μg/m³")
            else:
                print("Failed to read PM data")
                
            # Uncomment to run continuous reading
            print("Starting continuous reading (Ctrl+C to stop)...")
            reader.continuous_read(interval=2.0)
            
    except Exception as e:
        logger.error(f"Application error: {e}")

if __name__ == '__main__':
    main()
