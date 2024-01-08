# flake8: noqa
# Use this module for script reporting
#
# TME  11/06/18  Initial version
# TME  06/06/19  Use message type headings if more than one type of message is reported
# TME  08/15/19  Added support logging with the 'monitor+log'
# TME  03/27/20  Logging now appends to log file
# TME  04/08/20  Added log notify method to log without notifying the
#                Job Monitor. Also added status message counters.
# TME  04/30/20  Don't try to print job code when echo is set
# TME  06/16/20  report() now accepts a header message
# TME  01/24/22  Use FAILED rather than FAIL for status
# TME  04/13/23  Class will now report to the Job Monitor itself rather than 
#                using notifyJM.py. Retries are logged. Results message is 
#                written to log if Job Monitor is not responding.

#
# Load modules, set/initialize global variables
#
import logging, os, sys
from time import sleep
import requests

# To help find other directories that might hold modules or config files
libDir = os.path.dirname(os.path.realpath(__file__))

# Find and load any of our modules that we need
commonBin = libDir.replace('lib', 'bin')
logDir    = os.getenv("LOGFILE_PATH", "/home/etdadm/logs/etd_alma")
sys.path.append(commonBin)
from .ltstools import adminMailTo, adminMailFrom, get_date_time_stamp, jobMonitor, send_mail

# Use this class to track pass, fail and warning script messages and
# to report script results. 
class notify: # pragma: no cover

	def __init__(self, notifyMethod, jobCode = False, logFile = False):

		if notifyMethod == 'monitor' or notifyMethod == 'monitor+log':
			if jobCode:
				self.jobCode = jobCode
			else:
				print('A job code must be set to report to the Job Monitor')
				return None

		# Set up logging if specified. Path to a log file must be passed.
		if notifyMethod == 'monitor+log' or notifyMethod == 'log':
			if logFile:
				self.logFile = logFile
			else:
				print('A path to a log file must be set to log messages')
				return None

			logging.basicConfig(level=logging.INFO,
								format='%(asctime)s %(levelname)s %(message)s',
								filename=logFile,
								filemode='a')

		# Notification method
		self.notifyMethod = notifyMethod
	
		# To group messages by status type
		self.msgPass   = ''
		self.msgWarn   = ''
		self.msgFail   = ''
		self.countPass = 0
		self.countWarn = 0
		self.countFail = 0

	# Print message and save it as a fail, warn or pass type
	def log(self, type, message, echo = False):

		if echo: print(message)

		if type == 'fail':
			self.countFail += 1
			self.msgFail   += message + '\n'
			if 'log' in self.notifyMethod: logging.error(message)
		elif type == 'warn':
			self.countWarn += 1
			self.msgWarn   += message + '\n'
			if 'log' in self.notifyMethod: logging.warn(message)
		elif type == 'pass':
			self.countPass += 1
			self.msgPass   += message + '\n'
			if 'log' in self.notifyMethod: logging.info(message)
		else:
			if 'log' in self.notifyMethod: logging.info(message)

	# Report, or send, result message
	# Any messages are cleared
	def report(self, stage, echo = False, header = False):
		returnCode = True
		statusMsg  = 'Successful'
		statusCode = 'SUCCESS'
		
		if header:
			message    = f'{header}\n\n'
		else:
			message    = ''

		# Collect messages and figure out result
		if self.msgFail:
			message   += 'Failed\n' + self.msgFail + '\n'
			statusMsg  = 'Had failures'
			statusCode = 'FAILED'

		if self.msgWarn:
			if self.msgFail:
				message += '\n'
			else:
				statusMsg  = 'Had warnings'
				statusCode = 'WARNING'
			message += 'Warnings\n' + self.msgWarn + '\n'

		if self.msgPass:
			if self.msgWarn or self.msgFail: message += '\nSuccessful\n'
			message += self.msgPass + '\n'

		# Status code is also dependent on stage
		if stage == 'start':
			statusCode = 'STARTED_' + statusCode
		elif stage == 'running':
			if statusCode == 'SUCCESS':
				statusCode = 'RUNNING'
			elif statusCode == 'FAILED':
				statusCode = 'RUNNING_ERROR'
			else:
				statusCode = 'RUNNING_' + statusCode
		elif stage == 'stopped':
			statusCode = 'FAILED'
			statusMsg  = 'Failed'
			returnCode = False
		elif stage == 'complete':
			if statusCode == 'FAILED':
				statusCode = 'COMPLETED_' + statusCode
			else:
				statusCode = 'COMPLETED_' + statusCode

		# Print and report status result as specified
		if 'monitor' in self.notifyMethod: self.notifyJM(self.jobCode, statusCode, message, noRetries = True)
		if echo:
			print(statusMsg)
			print(message)

		# Clear messages
		self.msgPass   = ''
		self.msgWarn   = ''
		self.msgFail   = ''
		self.countPass = 0
		self.countWarn = 0
		self.countFail = 0

		return returnCode

	# notifyJM
	# Notify Job Monitor
	#
	# Parameters
	#   jobCode       Job Monitor job code such as "edi_orders" or "patload"
	#
	#   statusCode    Supported Job Monitor status codes are;
	#                     STARTED_SUCCESS
	#                     STARTED_WARNING
	#                     STARTED_FAIL
	#                     RUNNING
	#                     RUNNING_WARNING
	#                     RUNNING_ERROR
	#                     FAILED
	#                     COMPLETED_SUCCESS
	#                     COMPLETED_WARNING
	#                     COMPLETED_FAILED
	#
	#   message       Optional, use to pass a result message to the Job Monitor
	#
	#   runId         Optional, use to update a specific Job Monitor job run.
	#                 This parameter is not usually needed.
	#
	#   noRetries     Do not retry if Job Monitor fails to respond
	#
	def notifyJM(self, jobCode, statusCode, message = 'none', runId = False, noRetries = False):
		httpError = False

		# Number of attempts to notify the Job Monitor
		maxTries  = 6

		# Wait, in seconds, between tries. It will be doubled with each retry.
		retryWait = 60

		if runId:
			notifyJmUrl = '%s/set_job_status/job_code/%s/status_code/%s/run_id/%s' % (jobMonitor, jobCode, statusCode, runId)
		else:
			notifyJmUrl = '%s/set_job_status/job_code/%s/status_code/%s' % (jobMonitor, jobCode, statusCode)

		# If message is too large, write it to disk and report it
		if message:
			msgSize = len(message)
			if msgSize > 65535:
				msgInfo = self.write_log(message, logDir, jobCode)
				message = f'The results message was {msgSize} bytes which exceeds the size limitation of 65535 bytes. ' + msgInfo
				print(message)
		
		# Post status to the Job Monitor. Multiple attempts might be made.
		for loopCount in range(1, (maxTries + 1)):
			try:
				response = requests.post(notifyJmUrl, data = message, timeout = 15)
				
				if response.status_code == 200:
					(runId, discard) = response.text.split(',')
					httpError = False
					break
				else:
					httpError = response.text

			except Exception as e:
				httpError = e

			# Retry unless asked not to
			if noRetries:
				break
			else:
				if loopCount < maxTries:
					msgWarn = f'The Job Monitor did not respond. Another attempt will be made after in {retryWait} seconds.'
					print(msgWarn)
					if 'log' in self.notifyMethod: logging.warn(msgWarn)
					sleep(retryWait)
					retryWait += retryWait

		# If unable to report to the Job Monitor, write message to disk and then send mail
		if httpError:
			msgReturn  = 'Failed to notify the Job Monitor with a url of %s\n' % notifyJmUrl
			msgReturn += 'Http error was %s.' % (httpError)

			if message:
				msgInfo   = self.write_log(message, logDir, jobCode)
				msgReturn += f'\n\n{msgInfo}'

			mailTo   = adminMailTo
			mailFrom = adminMailFrom
			send_mail(mailTo, mailFrom, 'Failed to notify the Job Monitor', msgReturn)				
			return msgReturn

		return runId

	# Write message to a new log file
	def write_log(self, message, logDir, jobCode):
		from socket import getfqdn

		dateStamp = get_date_time_stamp()
		logFile = f'{logDir}/{jobCode}{dateStamp}'
		with open(logFile, 'w+') as log:
			log.write(message)
			
		hostname = getfqdn()
		message = f'The results message was written to {hostname}:{logFile}.\n'
		print(message)

		return message
			