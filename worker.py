import asyncio
import socket
import json
import subprocess
import abc


class indiv:

	co_workers = {}
	master_file = None
	def __init__(self, _manager, _type, _connection, _mfn = None):
		self.manager =  _manager
		self.connection = _connection
		self._connection = socket.socket(AF_INET,(connection[0],connection[1]))
		self.type_ = _type

		if _mfn is not None:
			master_file = _mfn
		

	def promotion(self):
		self.save()
		proc = subprocess.run(["python3.5",master_file])
		if proc.poll() is None:
			exit()

	@abc.abstractmethod
	def vote(self):
		pass

	@abc.abstractmethod
	def save(self):
		pass

	@abc.abstractmethod
	def do_work(self):
		pass

