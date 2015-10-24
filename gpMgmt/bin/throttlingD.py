#!/usr/bin/env python

import ConfigParser
import os
import mmap
import sys
import datetime
import time
import errno

class BackupFileHandle():
	def __init__(self, file_name, config_vals, log_file_handle):
		if(config_vals['EnableDirectIO'] == '1'):
			#open file for direct IO
			self.directIO_flag = True
			self.backup_file_name = file_name
			self.backup_file_handle = os.open(file_name, os.O_CREAT | os.O_DIRECT | os.O_TRUNC | os.O_RDWR)
			self.memory_map = mmap.mmap(-1, config_vals['ReadChunk'])
		else:
			self.directIO_flag = False
			self.backup_file_handle = open(file_name, "w")
		
		self.config_values = config_vals
		self.log_file_handle = log_file_handle
		self.line_i = 0
		self.total_chunk_size = 0

	def writeChunk(self, chunk):
		chunk_size=len(chunk)
		self.total_chunk_size+=chunk_size
		self.line_i+=1
		if(self.directIO_flag):
			if(chunk_size == self.config_values['ReadChunk']):
		
				if(self.log_file_handle):
					log_msg="First: LineSize=%s\tDataSize=%s\tLineCount=%s\n" %(chunk_size, self.total_chunk_size, self.line_i)
					self.log_file_handle.write(log_msg)
		
				#we just read a fulll chunk
				self.memory_map.write(chunk)
				os.write(self.backup_file_handle, self.memory_map)
				self.memory_map.seek(0)
			else:
		
				if(self.log_file_handle):
					log_msg="Second: LineSize=%s\tDataSize=%s\tLineCount=%s\n" %(chunk_size, self.total_chunk_size, self.line_i)
					self.log_file_handle.write(log_msg)

				#we read something but it less than 1 page
				os.close(self.backup_file_handle)
			
				#open the file regulary
				new_backup_file_handle = open(self.backup_file_name, "a")
				new_backup_file_handle.write(chunk)
				new_backup_file_handle.close()
		else: #No direct I/O
			self.backup_file_handle.write(chunk)
	
	def close(self):
		try:
			if(self.directIO_flag):
    				os.close(self.backup_file_handle)
			else:
				self.backup_file_handle.close()
		except OSError as oserr:
   			if oserr.args[0] == errno.EBADF:
            			#Closing file has closed file descriptor
				pass
			else:
				#Some other error
				raise oserr;

	def reset(self):
		self.total_chunk_size=0

class ConfigurationFileError(Exception):
	def __init__(self, file_name, msg):
		self.file = file_Name
		self.msg = msg
	def __str__(self):
		return "configuration file error: = %s %s" %(self.file, self.msg)
	
class ConfigValuesError(Exception):
	def __init__(self, option, value):
		self.option = option
		self.value = value
	def __str__(self):
		return "Illegal value = %s for configuration option = %s" %(self.value, self.option)

def TestBool(value):
	return (value=='0' or value=='1')

def TestMemoryChunk(value):
	int_value = int(value)
	return (int_value>=512 and int_value<=104857600)  #Check that Memory Chunks are between 0.5KB and 100MB


def TestAlignedMemoryChunk(value):
	int_value = int(value)
	return (TestMemoryChunk(int_value) and (int_value%512)==0)

def TestSleepTime(value):
	int_value = int(value)
	return (int_value>=0 and int_value<10) # check if the time is more than 0 and less than 10

def CheckConfigValues(test, value, default_value):
	if test(value):
		return value
	else:
		return default_value



def main(argv):
	config_values={'EnableDirectIO':1, 'EnableLog':0, 'LogFile':'/tmp/delay.output', 'SleepTime':1, 'SleepChunk':6291456, 'ReadChunk':20971520}
	GP_home = os.environ['GPHOME']
	config_file="%s/bin/throttlingD.cnf" %(GP_home)

	file_exist = os.path.isfile(config_file)

	if(file_exist):
		config_parser_defaults_dic={'SectionMain':{'EnableDirectIO':1, 'EnableLog':0, 'LogFile':'/tmp/delay.output','SleepTime':1,'SleepChunk':6291456}}
		config_parser = ConfigParser.ConfigParser(config_parser_defaults_dic)
	
		try: 
			config_parser.read(config_file)
		except ConfigParser.ParsingError:
			raise ConfiguratioFileError (config_file, "Configuration file parsing error")
	
		try:
			#Read all values from configuration file = "throttlingD.cnf"
			config_values['EnableDirectIO'] = CheckConfigValues(TestBool, config_parser.get('SectionMain', 'enableDirectIO'), config_values['EnableDirectIO'])
			config_values['EnableLog'] = CheckConfigValues(TestBool, config_parser.get('SectionMain', 'enableLog'),	config_values['EnableLog'])
			config_values['LogFile'] = config_parser.get('SectionMain', 'logFile')
			config_values['SleepTime'] = CheckConfigValues(TestSleepTime, int(config_parser.get('SectionMain', 'sleepTime')), config_values['SleepTime'])
			config_values['SleepChunk'] = CheckConfigValues(TestMemoryChunk, int(config_parser.get('SectionMain', 'sleepChunk')), config_values['SleepChunk'])
			config_values['ReadChunk'] = int(argv[0])*1024*1024; #Read chunk is a GUC parameter in MB
		
			#print config_values
		
		except ConfigParser.NoOptionError, option:
			#print No option in section
			raise ConfiguratioFileError (config_file, "No option in Section") 
			
		except ConfigParser.NoSectionError, section:
			#print No section
			raise ConfiguratioFileError (config_file, "No Section") 
		
		except ConfigParser.DuplicateSectionError, section:
			#print duplicate section
			raise ConfiguratioFileError (config_file, "Duplicate section") 
	else:
		#If configuration file not found print an error to stdout and exit
		raise ConfiguratioFileError (config_file, "Configuration file not found")


	backup_file_name = argv[1]
	log_file_handle=None
	if(config_values['EnableLog'] == '1'):
		try:
			log_file_handle = open(config_values['LogFile'], "w")
		except IOError as e:
			#In case that the log file cannot be opened --> Ignore the log
			pass

	# DirectI/O should be enabled in RHEL only
	if(config_values['EnableDirectIO']=='1'):
		# Check that we are running on RHEL. Otherwise disable directIO
		RHEL_file_exist = os.path.isfile('/etc/redhat-release')
		SUSE_file_exist = os.path.isfile('/etc/SuSE-release')
		if (not RHEL_file_exist and not SUSE_file_exist):
			config_values['EnableDirectIO']='0'
	
	backup_file_handle = BackupFileHandle(backup_file_name, config_values, log_file_handle)
	before_time=datetime.datetime.now()

	while True:	
		#for chunk in sys.stdin.read(int(config_values['ReadChunk'])):
		chunk = sys.stdin.read(config_values['ReadChunk'])
		if(not chunk):
			break;

		backup_file_handle.writeChunk(chunk)

		if (config_values['SleepTime']>0 and backup_file_handle.total_chunk_size >= config_values['SleepChunk']):
			after_time=datetime.datetime.now()
			time_diff =after_time-before_time
			
			if(log_file_handle):
				log_msg="timeDiff=%s\n" %(time_diff)
				log_file_handle.write(log_msg)
			
			if time_diff.seconds < config_values['SleepTime']:
				# need to sleep for the time delta
				sleep_time = float((config_values['SleepTime']*1000000)-time_diff.microseconds)/(config_values['SleepTime']*1000000)
				time.sleep(sleep_time)
				
				if(log_file_handle):
					log_msg="<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Sleep %f ms >>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n" %(sleep_time)
					log_file_handle.write(log_msg)
			
			backup_file_handle.total_chunk_size = 0
			before_time=datetime.datetime.now()
	
	backup_file_handle.close()
	
	if(log_file_handle):
		log_file_handle.close()

if __name__ == "__main__":
	main(sys.argv[1:])
