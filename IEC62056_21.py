import serial
import logging
import time
import re
from decimal import *
from string import Template
from collections import namedtuple
 
from BraceMessage import BraceMessage as __

IdentificationMessage = namedtuple("IdentificationMessage", "manufacturers_identification baudrate_identification identification protocol_mode baudrate")
DataSetMessage = namedtuple("DataSetMessage", "id value unit")

class InvalidMessageError(Exception):
	def __init__(self, message):
		self.message = message
	def __str__(self):
		return repr(self.message)

class TimeoutException(Exception):
	pass
	
class IEC62056_21:
	dataset_regex = re.compile("(?P<ID>[^\(\)/!]*)\((?P<Value>\d+(.\d)+)(\*(?P<Unit>[^\(\)/!]+))?\)")
	logger = logging.getLogger(__name__)
	ser_is_blocking = True
	
	min_reaction_time=0.2
	ctrl_StartChar = bytes('/')[0]
	ctrl_EndChar = bytes('!')[0]
	
	ctrl_EOF = bytes('\x21')[0] 

	ctrl_CTX = bytes('\x02')[0] # Start of text
	ctrl_ETX = bytes('\x03')[0] # End of text
	
	ctrl_ACK = bytes('\x06')[0] # Acknowledge
	ctrl_NAK = bytes('\x15')[0] # Negative acknowledge
	
	ctrl_SOH = bytes('\x01')[0] # Start of header
	
	def __init__(self, port, 
			baudrate=300, bytesize=serial.SEVENBITS, 
			parity=serial.PARITY_EVEN, stopbits=serial.STOPBITS_ONE):
		
		self.ser = serial.Serial(port, baudrate, bytesize, parity, stopbits,timeout=self.min_reaction_time*2)
		self.logger.debug(__("Using port '{port}', baudrate {baudrate}.", 
			port = self.ser.portstr, baudrate=self.ser.baudrate))

	def close(self):		
		self.ser.close()
		self.ser = None
	
	def read(self):
		self.ser.baudrate = 300

		msg_signon = bytearray("/?!\r\n")
		
		# 1 start bit + parity + 1 stop bit
		expected_delay = len(msg_signon)*1.0 / (self.ser.baudrate / (self.ser.bytesize + 3));

		start = time.time()
		self.ser.write(msg_signon)
		self.ser.flush()
				
		end = time.time()
		actual_delay = (end-start)
		if actual_delay < expected_delay:
			self.ser_is_blocking = False
			self.logger.info(__("Non blocking writes. Expected: {expected_duration:3}, actual delay: {duration:3.1}", 
				expected_duration = expected_delay, duration=actual_delay,
				))
			time.sleep(expected_delay-actual_delay)

		identification_message = self._read_identification_message()
		self.logger.debug(identification_message)
		
		self._write_handshake(identification_message)

		for dataset in self._read_data_message():
			yield self._read_dataset_structure(dataset)		
	
	def _write_blocking(self, data):
		# 1 start bit + parity + 1 stop bit
		expected_delay = len(data) * 1.0 / (self.ser.baudrate / (self.ser.bytesize + 3));

		w = self.ser.write(data)
		start = time.time();
		
		self.ser.flush()

		delay = expected_delay - (time.time() - start)
		if not self.ser_is_blocking and delay > 0:
			time.sleep(delay)

	def _read_identification_message(self):
		msg = self._read_dataline()

		if len(msg) == 0:
			raise TimeoutException
			
		if not msg[0] == ord('/'):
			while True:
				b = self.ser.read()
				if len(b) == 0:
					break
				msg.append(b)
					
			raise InvalidMessageError("Missing start character, got: " + msg)
		
		if len(msg) < 6:
			raise InvalidMessageError("Identification message to short < 5: {0}".format(msg))


		manufacturers_identification = msg[1:4].decode('ascii')
		baudrate_identification = chr(msg[4])
		if msg[5] == '\\':
			if len(msg) < 8:
				raise InvalidMessageError("Identification message to short < 8: {0}".format(msg))
			identification = msg[7:len(msg)-2].decode('ascii')
		else:
			identification = msg[5:len(msg)-2].decode('ascii')
			
		baudrateselection = [300, 600, 1200, 2400, 4800, 9600, 19200,
			0, 0, 0, #reserved
			600, 1200, 2400, 4800, 9600, 19200]
				
		protocol_mode = 'A'
		if baudrate_identification > 'A' and baudrate_identification <= 'I':
			protocol_mode = 'B'
		if baudrate_identification > '0' and baudrate_identification <= '9':
			protocol_mode = 'C'
			if msg[5] == '\\':
				protocol_mode = 'E'	
		
		baudrate = baudrateselection[int(baudrate_identification, 16)]
		

		result = IdentificationMessage(
			manufacturers_identification, baudrate_identification, identification, protocol_mode, baudrate)
		
		return result
		
	def _read_data_message(self):
		b = self.ser.read()
		
		if len(b) == 0:
			raise TimeoutException
			
		if b[0] != self.ctrl_CTX:
			error = "No STX frame start character: " + repr(b)
			raise InvalidMessageError(error)
		
		b = self.ser.read()
		while b <> self.ctrl_EOF:
                        if len(b) > 0:
			    dataline = self._read_dataline(bytearray([b]))
			    yield dataline
			
			b = self.ser.read()

		b = self.ser.read()
		b = self.ser.read()
		b = self.ser.read()
		if b[0] != self.ctrl_ETX:
			error = "No ETX frame end character: " + repr(b)
			raise InvalidMessageError(error)
			
		b = self.ser.read()
		self.logger.debug(__("BCC Value: {bcc}", bcc=b))
			
	def _read_dataset_structure(self, dataset):
		result = self.dataset_regex.match(dataset)
		if result == None:
			error = "Unable to parse dataset structure: " + dataset
			raise InvalidMessageError(error)

		id = str(result.group("ID"))
		value = Decimal(str(result.group("Value"))) 
		
		if (result.group("Unit") == None):
			unit = None
		else:
			unit = str(result.group("Unit"))
		
		message = DataSetMessage(id, value, unit)
		return message
		
	def _write_handshake(self, 	identification_message):
		data = bytearray([0x06, '0', identification_message.baudrate_identification, ord('0'), 0x0D, 0x0A]);
		self._write_blocking(data)
		time.sleep(self.min_reaction_time)
		self.ser.baudrate = identification_message.baudrate
		
	def _read_dataline(self, msg = None):
		if msg == None:
			msg = bytearray()
		
		eom = '\r\n'
		m = 0
		
		b = self.ser.read()
		while len(b) > 0:
			msg.append(b)

			if eom[m] == b:
				m += 1
			else:
				m = 0

			if m == len(eom):
				msg.pop(len(msg)-1)
				msg.pop(len(msg)-1)
				break
				
			b = self.ser.read()
		
		return msg
