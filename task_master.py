import re
import socket
import asyncio
import subprocess
from collections import deque
import uuid

#Check time since last error in system errors to increase NUM_ERRORS its certain that errors will arise over time, however cato
#-strophic errors releastically would create a large ammount of errors in a rather small amount of time

class TaskTypeError(Exception):
	#Raised when function arguments do not match the type of task they should be applied to.
	def __init__(self,value):
		self.value = value
	def __str__(self):
		return repr(self.value)

class SystemicIssueError(Exception):
	#Raised when Taskmaster class's error limit (Set in the configuration file) has been reached. 
	#This implies a catostrophic system error has occurred.
	#Rather than continue to spin wheels the error will be logged, the voting system on the worker nodes will shortly be enacted, 
	#a worker will be promoted to the main node and a worker process will be spawned on the former manager instance (if availible)
	#A seperate catostrophic error counter will be increased in the system script running this program.
	#When 5 catostrophic errors are reached the system script will kill the program entirely so it can be
	#independently checked by a programmer to assess possible bugs.

	def __init__(self,value):
		self.value = value
	def __str__(self):
		return repr(self.value)

class __TASKMASTER__:
	
	

	
	def __init__(self,initFileName,urlFileName):
			self.SETTINGS = {"SPEED": 0,"DEPTH": 8,"TIMEOUT": 10,"MAX_CHILDREN": 10, "ERROR_TOLERANCE": 100}
			self._TASK = {"ID": 0, "SETTINGS": "", "URL": "", "CANDIDATE": "", "WORKER": "", "DATA": None, "TYPE": ""}
			self.IP_RANGE = {"MIN": 0, "MAX": 10};
			self._NUM_ERRORS = 0
			self.TASKS = deque() 
			self.CHUNKS = deque()
			self.IDLE = deque()
			self.IDS = {}
			self.FREERANGE = 0
			self.FINISHED_TASKS = []
			self.FINISHED_CTASKS = deque()
			self.FINISHED_LTASKS = deque()
			self.FINISHED_STASKS = []
			self.ERRORED_TASKS = []		
			self._CONNECTED = 0
			self.CONNECTED = {}
			self.BUSY = deque()
			self.URLS = deque()
			self.WORDS = []
			self.CANDIDATES = []
			initP2 = False
			initP3 = False
			lines = [currentLine.rstrip('\n') for currentLine in open(initFileName,"r+")]
			for x in lines:
				if re.match("/([Candidates:]{11})/",x) is None:
					initP2 = True
					continue
				if re.match("/([Settings:]{9})/",x) is None:
					initP3 = True
					initP2 = False
					continue
				if not initP2 and not initP3:
					self.WORDS.append(re.split(",",x))
				else: 
					if initP2:
						self.CANDIDATES.append(re.split(",",x))
					else:
						tempSet = []
						tempSet.append(re.split(" ",x))
						if tempSet[0] in SETTINGS:
							self.SETTINGS[tempSet[0]] = int(tempSet[2])
			urlLines = [currentLine.rstrip('\n') for currentLine in open(urlFileName,"r+")]
			for x in urlLines:
				URLS.append(x)

	async def createNewCrawlBatch(self, callback = None):
		for x in range(0,_CONNECTED):
			count = 0
			urls = []
			while count < 5 and not URLS.empty():
				urls.append(self.URLS.pop())
				++count
			await createTasks(urls,"CTASK")
		if callback is not None:
			callback();


	def checkID(self, _id):
		for x in self.IDS:
			if _id.equals(x):
				return x
		else: return None

	async def createTasks(self, urls, _type, finishedTask = None, callback = None):
		try:
			if _type.equals("CTASK") and finishedTask is not None:
				++self.NUM_ERRORS
				raise TaskTypeError("finishedTask parameter is not None and _type is CTASK!")
		except TaskTypeError as e:
			print("TaskTypeError exception occured, reason: " + e.value)
		else:
			if _type.equals("CTASK"):
				for x in urls:
					newCTask = copy.deepcopy(self._TASK)
					newCTask["TYPE"] = "CTASK"
					_id = uuid.uuid4() #Blocking?
					IDS.append(_id)
					newCTask["ID"] = _id
					newCTask["URL"] = x
					newCTASK["SETTINGS"] = self.SETTINGS
					self.TASKS.append(newCTask)

			
			if _type.equals("LTASK"):
				data = finishedTask["DATA"]
				for x in data:
					newLTask = copy.deepcopy(self._TASK)
					newLTask["TYPE"] = "LTASK"
					_id = uuid.uuid4()
					self.IDS.append(_id)
					newLTask["ID"] = _id
					newLTask["DATA"] = x["DATA"]
					newLTask["URL"] = x["URL"];
					self.TASKS.append(newLTask)

			if _type.equals("STASK"):
				data = finishedCTask["DATA"]
				for x in data:
					newLTask = copy.deepcopy(self._TASK)
					newLTask["TYPE"] = "LTASK"
					_id = uuid.uuid4()
					self.IDS.append(_id)
					newSTask["ID"] = _id
					newSTask["DATA"] = x["DATA"]
					newSTask["CANDIDATE"] = x["CANDIDATE"]
					newSTask["URL"] = x["URL"]
					self.TASKS.append(newSTask)

	async def createChunks(self, callback = None):
		chunk = {"TYPE": None, "TASKS": [], WORKER: None}
		startOfChunk = False
		_type = None
		while not Tasks.empty():
			cTask = Tasks.pop()
			if startOfChunk is False:
				_type = cTask["TYPE"]
				chunk["TYPE"] = _type
				chunk["TASKS"].append(cTask)
				startOfChunk = True
			else:
				if not cTask["TYPE"].equals(_type):
					self.CHUNKS.append(chunk)
					chunk = {"TYPE": None, "TASKS": []}
				else:
					chunk["TASKS"].append(cTask)
		if callback is not None:
			callback()
	
	async def allocateWorkers(self, callback = None):
		if CHUNKS.empty():
			await createNewCrawlBatch()
		while not IDLE.empty() and not CHUNKS.empty():
			worker = IDLE.pop()
			chunk = CHUNKS.pop()
			BUSY.append(worker)
			worker.do(chunk)

		if callback is not None:
			callback()

	async def createWorkers(self, num):
		#192.168.1.x 
		for x in range(self.IP_RANGE["MIN"],self.IP_RANGE["MAX"]):
			IP = "192.168.1."+str(170+x)
			COMMAND = "python ~/usr/desktop/worker.py"
			if self.CONNECTED.get(IP) is not None:
				continue
			if x > self.SETTINGS["MAX_CHILDREN"] or x > num:
				self.FREERANGE = iprange-x
				break
			try:
				sub =  await BaseEventLoop.subprocess_exec("ssh","%s" % IP, COMMAND, shell = False, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
				err = sub.stderr.read()
				sub.close()
				if err is not []:
					raise RunTimeError(err) 
			except Exception as e:
					print(e)
					#ADD LOGGING CODE HERE
					continue
			finally:
				CONNECTED.append(IP)
				IDLE.append(IP)

	def checkStartedWorkers(self, iprange):
		#192.168.1.x 
		for x in range(0,iprange):
			IP = "192.168.1."+str(170+x)
			try:
				connection = ""
			except Exception as e:
				raise e
			finally:
				pass
				


	def killswitchengage(self):
		x = "Silence fills the empty grave, now that I have gone. But my mind is not at rest, for questions linger on."
		sys.exit(-1);







			



	









						




