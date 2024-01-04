# flake8: noqa
#
# Common LTS script routines and variables 
#
# TME  06/22/18  Initial version
# TME  12/07/18  Added send_mail_attachment()
# TME  07/09/19  Using SafeLoader with yaml module
# TME  09/12/19  Added get_date_time_stamp()
# TME  10/31/19  Added privateKey and try/excepts
# TME  01/24/20  Added support for the gpg parameters
# TME  03/27/20  Added monthly precision get_date_time_stamp
# TME  04/07/21  Added oracleHome
# TME  08/12/21  Added support for Footprints API
# TME  08/23/21  Remove make_fp_ticket() since it used hulmail.
#                Added get_mail_list()
# TME  06/30/22  Now using mailhub.harvard.edu as mail host
# TME  10/05/22  Removed variables used with Footprints
# TME  10/13/22  Added year precision to get_date_time_stamp()
# TME  12/13/22  Added is_holiday()
# TME  12/14/22  Added is_winter_break()

#
# Load modules, set/initialize global variables
#
import os, sys

'''
# To help find other directories that might hold modules or config files 
libDir      = os.path.dirname(os.path.realpath(__file__))
confDir     = libDir.replace('lib', 'conf')
mailListDir = libDir.replace('lib', 'mail_lists')
sys.path.append(confDir)
'''

jobMonitor = os.getenv('jobMonitor')
adminMailTo = os.getenv('adminMailTo')
adminMailFrom = os.getenv('adminMailTo')
		

def get_date_time_stamp(precision = 'minute'):  # pragma: no cover
	from datetime import datetime
	from time import time

	if precision == 'minute':
		dateTimeStamp = datetime.fromtimestamp(int(time())).strftime("%Y%m%d%H%M")
	elif precision == 'second':
		dateTimeStamp = datetime.fromtimestamp(int(time())).strftime("%Y%m%d%H%M%S")
	elif precision == 'hour':
		dateTimeStamp = datetime.fromtimestamp(int(time())).strftime("%Y%m%d%H")
	elif precision == 'day':
		dateTimeStamp = datetime.fromtimestamp(int(time())).strftime("%Y%m%d")
	elif precision == 'month':
		dateTimeStamp = datetime.fromtimestamp(int(time())).strftime("%Y%m")
	elif precision == 'year':
		dateTimeStamp = datetime.fromtimestamp(int(time())).strftime("%Y")
	else:
		dateTimeStamp = None

	return dateTimeStamp

'''
# get_mail_list
#
# Parameters
#   mailList	Mailing list to fetch
#	returnType	Choices are; "string" to return email addresses as a string
#				separated by commas or "list" to get them as a Python list.
#
# Returns
#   A mailing list upon success, otherwise, False.
#
def get_mail_list(mailList, returnType):
	import re
	
	if re.match('.*\.txt', mailList):
		mailListFile = f'{mailListDir}/{mailList}'
	else:
		mailListFile = f'{mailListDir}/{mailList}.txt'
	
	# Read file into a list without newlines
	with open(mailListFile) as input:
		addresses = input.read().splitlines()
			
	if returnType == 'string':
		addresses = ','.join(addresses)
	elif returnType == 'list':
		pass
	else:
		print(f'returnType of {returnType} is not supported')
		return False
			
	return addresses
'''

'''
# is_holiday
# Returns True if datestamp falls on a Harvard holiday,
# otherwise, returns False
#
# Optional parameters
#   notifyJM     A notify object used to log messages for the Job Monitor
#   datestamp    Datestamp to check in the form of YYYYMMDD
#
def is_holiday(notifyJM = False, datestamp = False):
	from holidays import holidays, updatedTil

	if not datestamp:
		datestamp = get_date_time_stamp('day')

	# Notify is the holidays lookup table is out of date
	if datestamp > updatedTil:
		message = 'The holidays configuration tables needs to be updated'
		if notifyJM:
			notifyJM.log('fail', message, True)
		else:
			print(message)
						
	if datestamp in holidays:
		return True
	else:
		return False
'''

'''
# is_winter_break
# Returns True if datestamp falls with in our winter break,
#  otherwise, returns False
#
# Optional parameters
#   notifyJM     A notify object used to log messages for the Job Monitor
#   datestamp    Datestamp to check in the form of YYYYMMDD
#
def is_winter_break(notifyJM = False, datestamp = False):
	# from holidays import winterBreak, updatedTil

	if not datestamp:
		datestamp = get_date_time_stamp('day')

	# Notify is the holidays lookup table is out of date
	if datestamp > updatedTil:
		message = 'The holidays configuration tables needs to be updated'
		if notifyJM:
			notifyJM.log('fail', message, True)
		else:
			print(message)
						
	if datestamp in winterBreak:
		return True
	else:
		return False
'''

# notify
# Notify user, by email if specified.
#
# Parameters
#   result      This will appear in the body of the email and subject line.
#   job         Such as in "EDO Load Files". This will
#               appear in the body of the email and subject line.
#   message     Optional. Any addtional information to show the user, it will be seen
#               in the body of the email. Can be left empty.
#   mailTo      Optional. Email address to send message to.
#
def notify(result, job, message = False, mailTo = False):  # pragma: no cover

    if mailTo:
        if message:
            mailMsg = message
        else:
            mailMsg = job, result
            
        send_mail(mailTo, adminMailFrom, '%s %s' % (job, result), '%s' % (mailMsg))
    
    print(job, result)
    if message: print(message)

    return

# send_mail
#
# Parameters
#   mailTo       Email address to send message to
#   mailFrom     Email address to use as the 'From'
#   subject      Message to appear in subject line
#   message      Optional, the subject we be used in the
#                body of the email if message is not set
#
def send_mail(mailTo, mailFrom, subject, message = False):  # pragma: no cover
    import smtplib, re
    from email.mime.text import MIMEText

    # Create a text/plain message
    if message:
        msgEmail = MIMEText(message)
    else:
        msgEmail = MIMEText(subject)
    
    msgEmail['Subject'] = subject
    msgEmail['From']    = mailFrom
    msgEmail['To']      = mailTo
        
    # Convert scalar to array if more than 1 address is used
    matched = re.match('.+,.+', mailTo)
    if matched != None:
        mailTo = mailTo.split(',')
        
    smtp = smtplib.SMTP('mailhub.harvard.edu')
    smtp.sendmail(mailFrom, mailTo, msgEmail.as_string())
    smtp.quit()            

    return True

# send_mail_attachment
# Send email with attachments
#
# Parameters
#   mailTo       Email address(es) to send message to. Pass in as a list.
#   mailFrom     Email address to use as the 'From'
#   subject      Message to appear in subject line
#   message      Optional, the subject we be used in the
#                body of the email if message is not set
#
def send_mail_attachment(mailTo, mailFrom, subject, message, files = None):  # pragma: no cover
	import smtplib, re
	from os.path import basename
	from email.mime.application import MIMEApplication
	from email.mime.multipart import MIMEMultipart
	from email.mime.text import MIMEText
	from email.utils import COMMASPACE, formatdate

	msgEmail = MIMEMultipart()
	msgEmail['From']    = mailFrom
	msgEmail['To']      = mailTo
	msgEmail['Date']    = formatdate(localtime=True)
	msgEmail['Subject'] = subject

	# Convert scalar to array if more than 1 address is used
	matched = re.match('.+,.+', mailTo)
	if matched != None:
		mailTo = mailTo.split(',')
        
	# files needs to be converted to a list regardless
	matched = re.match('.+,.+', files)
	if matched == None:
		files = [files]
	else:
		files = files.split(',')
        
	msgEmail.attach(MIMEText(message))
	
	# Attach any files (or not)
	for file in files or []:
		with open(file, "rb") as attachment:
			part = MIMEApplication(
				attachment.read(),
				Name=basename(file)
			)
	
		# After the file is closed
		part['Content-Disposition'] = 'attachment; filename="%s"' % basename(file)
		msgEmail.attach(part)

	smtp = smtplib.SMTP('mailhub.harvard.edu')
	smtp.sendmail(mailFrom, mailTo, msgEmail.as_string())
	smtp.close()
