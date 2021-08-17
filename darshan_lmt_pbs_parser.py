import pdb
import sys
#sys.path.append('/global/homes/s/sgkim/pytokio-master')
#sys.path.append('/global/project/projectdirs/m1248/cilioh14/pytokio')
import time
import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams.update({'font.size': 14})
import datetime
#import tokio.tools
import pandas
import numpy
import math
import os
import multiprocessing
from multiprocessing import Manager, Process
import sqlite3
import re
import datetime
import subprocess
import traceback
import scandir

from imp import reload
reload(sys)

#os.chdir("/global/u2/s/sgkim/codes/darshan_code_skim/")
#dir = "/global/cscratch1/sd/tengwang/miner0612/parsed_darshan/2017"
#dir = "/global/project/projectdirs/m1248/sgkim/h5write"
#dir = "/global/project/projectdirs/m1248/sgkim/parsed_darshan"
#dir = "/global/cscratch1/sd/sbyna/logs/darshan/Cori_archive_2018/parsed_darshan"
#localdir = "/global/u2/s/sgkim/codes/darshan_code_skim/"
#localdir = "/global/project/projectdirs/m1248/cilioh14/darshan_data/"
localdir = "./"
logdir = "./logs_one"
darshandir = "./darshan_log"
app = "total"
#sqlite cannot accpet - as table name
app_sqlite = app.replace('-','_')
#set ouput dir
outputdir = localdir + app_sqlite + ".sql"

#delete existing file
#if os.path.exists(localdir + outputdir):
#		os.remove(localdir + outputdir)

#if os.path.exists(localdir + app_sqlite + ".db"):
#		os.remove(localdir + app_sqlite + ".db")

#if os.path.exists(localdir + "skipped.txt"):
#		os.remove(localdir + "skipped.txt")

conn = sqlite3.connect(localdir + app_sqlite + ".db")

c = conn.cursor()
c.execute("DROP TABLE IF EXISTS " + app_sqlite)
c.execute('CREATE TABLE ' + app_sqlite
		  + """ (darshanDir text, progName text, userID text, jobID int, startTime DATETIME, endTime DATETIME, ioStartTime int, runTime int, numProc int, numCPU int, numNode int,
  numOST int, stripeSize int, knl int,
  totalFile int, totalIOReqPOSIX int, totalMetaReqPOSIX int,
  totalIOReqMPIIO int, totalMetaReqMPIIO int, totalIOReqSTDIO int, totalMetaReqSTDIO int,
  mdsCPUMean float, mdsCPU95 float, ossReadMean float, ossRead95 float, ossWriteMean float, ossWrite95 float,
  ossReadHigher1g float, ossReadHigher4g float, ossWriteHigher1g float, ossWriteHigher4g float,
  totalFilePOSIX int, totalFileMPIIO int, totalFileSTDIO int, seqWritePct float, seqReadPct float, consecWritePct float, consecReadpct float,
  writeLess1k float, writeMore1k float, readLess1k float, readMore1k float,
  writeLess1m float, writeMore1m float, readLess1m float, readMore1m float,
  writeBytesTotal float, readBytesTotal float, writeRateTotal float, readRateTotal float,
  writeBytesPOSIX float, writeTimePOSIX float, writeRatePOSIX float, readBytesPOSIX float, readTimePOSIX int, readRatePOSIX float,
  writeBytesMPIIO float, writeTimeMPIIO float, writeRateMPIIO float, readBytesMPIIO float, readTimeMPIIO int, readRateMPIIO float,
  writeBytesSTDIO float, writeTimeSTDIO float, writeRateSTDIO float, readBytesSTDIO float, readTimeSTDIO int, readRateSTDIO float,
  ReadReqPOSIX int, WriteReqPOSIX int, OpenReqPOSIX int, SeekReqPOSIX int, StatReqPOSIX int, readTimePOSIXonly float, writeTimePOSIXonly float, readTimeMPIIOonly float, writeTimeMPIIOonly float, readTimeSTDIOonly float, writeTimeSTDIOonly float, metaTimePOSIX float, metaTimeMPIIO float, metaTimeSTDIO float,
  slowWriteTimePOSIX float, slowReadTimePOSIX float, slowWriteTimeMPIIO float, slowReadTimeMPIIO float, IndOpenReqMPIIO int, ColOpenReqMPIIO int, IndWriteReqMPIIO int, ColWriteReqMPIIO int, IndReadReqMPIIO int, ColReadReqMPIIO int,
  SplitReadReqMPIIO int, SplitWriteReqMPIIO int, NbReadReqMPIIO int, NbWriteReqMPIIO int,
  OpenReqMPIIO int, ReadReqMPIIO int, WriteReqMPIIO int,
  OpenReqSTDIO int, ReadReqSTDIO int, WriteReqSTDIO int, SeekReqSTDIO int, FlushReqSTDIO int,
  ostlist text, ossWriteLargest float, ossReadLargest float, ossWriteStart95 float, ossWriteStartLargest float, writeBytesPct float)""")


mdsCPU_df = ossRead_df = ossWrite_df = 0

result_list = []
def process(file):
	fullDir = file
	file = os.path.basename(file)
	insertString = ""

	if True:
		#print("DK: first if")
		if 1:
			#print("DK: second if")
			progName = userID = jobID = startTime = endTime = runTime = numProc = numOST = stripeSize  = -1
			ioStartTime = -1
			totalFilePOSIX = totalFileMPIIO = totalFileSTDIO = 0
			writeRatePOSIX = readRatePOSIX = 0
			writeRateSTDIO = readRateSTDIO = 0
			writeRateMPIIO = readRateMPIIO = 0
			writeBytesPOSIX = writeTimePOSIX = readBytesPOSIX = readTimePOSIX =  0
			writeBytesMPIIO = writeTimeMPIIO = readBytesMPIIO = readTimeMPIIO =  0
			writeBytesSTDIO = writeTimeSTDIO = readBytesSTDIO = readTimeSTDIO =  0
			slowWriteTimePOSIX = slowReadTimePOSIX = 0
			slowWriteTimeMPIIO = slowReadTimeMPIIO = 0
			IndOpenReqMPIIO = ColOpenReqMPIIO = IndWriteReqMPIIO = ColWriteReqMPIIO = IndReadReqMPIIO = ColReadReqMPIIO = 0
			SplitReadReqMPIIO = SplitWriteReqMPIIO = NbReadReqMPIIO = NbWriteReqMPIIO = 0
			OpenReqMPIIO = ReadReqMPIIO = WriteReqMPIIO = 0
			OpenReqSTDIO = WriteReqSTDIO = ReadReqSTDIO = SeekReqSTDIO = FlushReqSTDIO = 0
			writeBytesTotal = writeRateTotal = 0
			metaTimePOSIX = metaTimeMPIIO = metaTimeSTDIO = 0
			readBytesTotal = readRateTotal = 0
			WriteReqPOSIX = ReadReqPOSIX = 0
			seqWriteReqPOSIX = seqReadReqPOSIX = 0
			seqWritePct = seqReadPct = 0
			consecWriteReqPOSIX = consecReadReqPOSIX = 0
			consecWritePct = consecReadPct = 0
			readLess1k = readMore1k = writeLess1k = writeMore1k = 0
			readLess1m = readMore1m = writeLess1m = writeMore1m = 0
			OpenReqPOSIX = SeekReqPOSIX = StatReqPOSIX = 0
			readTimePOSIXonly = writeTimePOSIXonly = 0
			readTimeMPIIOonly = writeTimeMPIIOonly = 0
			readTimeSTDIOonly = writeTimeSTDIOonly = 0
			totalFile = totalIOReqPOSIX = totalMetaReqPOSIX = 0
			totalIOReqMPIIO = totalMetaReqMPIIO = totalIOReqSTDIO = totalMetaReqSTDIO = 0
			isknl = 0;
			usedOST = list()
			ostlist = ""
			writeBytesPct = 0
			# progName = os.path.basename(file)[file.find("_") + 1 : file.find(tempID) - 1]
			# userID = os.path.basename(file)[:file.find("_")]
			# jobID = re.findall(r"id+\d+", os.path.basename(file))[0][2:]
			progName = "test"
			userID = "dk"
			jobID = 1234
			#jiwoo
			#print(progName + " " + userID + " " + jobID)

			with open(fullDir, 'r', encoding='utf-8') as f:
				#print("DK: oepn full dir - %s" % (fullDir))
				for line in f:
					#print(line)
					if "start_time_asci: " in line:
						startTime = line.split(": ",1)[1][:-1]
						temp = re.findall(r'\S+', startTime)
						startTime = temp[1] + " " + temp[2] + " " +  temp[3] + " " + temp[4]
						startTime = datetime.datetime.strptime(startTime, '%b %d %H:%M:%S %Y')
					if "run time: " in line:
						runTime = line.split(": ",1)[1][:-1]
					if "nprocs: " in line:
						numProc = line.split(": ",1)[1][:-1]
					if ("total_POSIX_F_OPEN_START_TIMESTAMP: " in line) or ("total_POSIX_F_READ_START_TIMESTAMP: " in line) or ("total_POSIX_F_WRITE_START_TIMESTAMP: " in line):
						if ioStartTime == -1 or ioStartTime > float(line.split(" ")[1]):
							ioStartTime = float(line.split(" ")[1])
					if "total: " in line:
						if int(line.split(" ")[3]) == int((writeBytesPOSIX + readBytesPOSIX)*1000000):
							totalFilePOSIX = line.split(" ")[2]
						elif int(line.split(" ")[3]) == int((writeBytesMPIIO + readBytesMPIIO)*1000000):
							totalFileMPIIO = line.split(" ")[2]
						elif int(line.split(" ")[3]) == int((writeBytesSTDIO + readBytesSTDIO)*1000000):
							totalFileSTDIO = line.split(" ")[2]
					if "LUSTRE_OST_ID_" in line and not line.startswith("#"):
						#temp = int(line.split("\t")[3][14:]) + 1
						#temp = int(line.split()[3][14:]) + 1
						temp = int((line.split())[3][14:])
						#set numOST as the highest OST
						#numOST may vary accroding to file or directory
						#print(line)
						#print(int((line.split())[3][14:]))
						#if int(line.split("\t")[4]) not in usedOST:
						#if int(line.split()[4]) not in usedOST:
						#	usedOST.append(int(line.split()[4]))
						if temp not in usedOST:
							usedOST.append(temp)
						#print(usedOST)
					if "LUSTRE_STRIPE_SIZE" in line and not line.startswith("#"):
						temp = int(line.split()[4])
						#set stripeSize as the max stripe sizeT
						#stripesize may vary accroding to file or directory
						if temp > stripeSize:
							stripeSize = temp
					#POSIX request Size
					if "total_POSIX_SIZE_READ_0_100: " in line:
						readLess1m = readLess1m + int(line.split(" ")[1])
						readLess1k = readLess1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_READ_100_1K: " in line:
						readLess1m = readLess1m + int(line.split(" ")[1])
						readLess1k = readLess1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_READ_1K_10K: " in line:
						readLess1m = readLess1m + int(line.split(" ")[1])
						readMore1k = readMore1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_READ_10K_100K: " in line:
						readLess1m = readLess1m + int(line.split(" ")[1])
						readMore1k = readMore1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_READ_100K_1M: " in line:
						readLess1m = readLess1m + int(line.split(" ")[1])
						readMore1k = readMore1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_READ" in line:
						readMore1m = readMore1m + int(line.split(" ")[1])
						readMore1k = readMore1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_WRITE_0_100: " in line:
						writeLess1m = writeLess1m + int(line.split(" ")[1])
						writeLess1k = writeLess1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_WRITE_100_1K: " in line:
						writeLess1m = writeLess1m + int(line.split(" ")[1])
						writeLess1k = writeLess1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_WRITE_1K_10K: " in line:
						writeLess1m = writeLess1m + int(line.split(" ")[1])
						writeLess1k = writeLess1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_WRITE_10K_100K: " in line:
						writeLess1m = writeLess1m + int(line.split(" ")[1])
						writeMore1k = writeMore1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_WRITE_100K_1M: " in line:
						writeLess1m = writeLess1m + int(line.split(" ")[1])
						writeMore1k = writeMore1k + int(line.split(" ")[1])
					if "total_POSIX_SIZE_WRITE" in line:
						writeMore1m = writeMore1m + int(line.split(" ")[1])
						writeMore1k = writeMore1k + int(line.split(" ")[1])

					#ADD METATIME to write/read time
					if "total_POSIX_F_META_TIME: " in line and not line.startswith("#"):
						metaTimePOSIX = float(line.split(" ")[1])
					#jiwoo
					if "total_MPIIO_F_META_TIME: " in line and not line.startswith("#"):
						metaTimeMPIIO = float(line.split(" ")[1])
					if "total_STDIO_F_META_TIME: " in line and not line.startswith("#"):
						metaTimeSTDIO = float(line.split(" ")[1])

					#POSIX I/O Rate
					#writeRatePOSIX
					if "total_POSIX_BYTES_WRITTEN:" in line and not line.startswith("#"):
						writeBytesPOSIX = float(line.split(" ")[1])/1000000
						#TEST only go through write bytes POSIX > 10G
					#jiwoo
					if "total_POSIX_F_WRITE_TIME:" in line and not line.startswith("#"):
						writeTimePOSIX = float(line.split(" ")[1])
						writeTimePOSIXonly = writeTimePOSIX
					if "total_POSIX_F_MAX_WRITE_TIME:" in line and not line.startswith("#"):
						slowWriteTimePOSIX = float(line.split(" ")[1])
						#writeTimePOSIXonly = writeTimePOSIX
					#readRatePOSIX
					if "total_POSIX_BYTES_READ:" in line and not line.startswith("#"):
						readBytesPOSIX = float(line.split(" ")[1])/1000000
					#jiwoo
					if "total_POSIX_F_READ_TIME:" in line and not line.startswith("#"):
						readTimePOSIX = float(line.split(" ")[1])
						readTimePOSIXonly = readTimePOSIX
					if "total_POSIX_F_MAX_READ_TIME:" in line and not line.startswith("#"):
						slowReadTimePOSIX = float(line.split(" ")[1])
						#readTimePOSIXonly = readTimePOSIX

					#MPIIO I/O Rate
					#writeRateMPIIO
					if "total_MPIIO_BYTES_WRITTEN:" in line and not line.startswith("#"):
						writeBytesMPIIO = float(line.split(" ")[1])/1000000
					if "total_MPIIO_F_WRITE_TIME:" in line and not line.startswith("#"):
						writeTimeMPIIO = float(line.split(" ")[1])
						writeTimeMPIIOonly = writeTimeMPIIO
					if "total_MPIIO_F_MAX_WRITE_TIME:" in line and not line.startswith("#"):
						slowWriteTimeMPIIO = float(line.split(" ")[1])
					#readRateMPIIO
					if "total_MPIIO_BYTES_READ:" in line and not line.startswith("#"):
						readBytesMPIIO = float(line.split(" ")[1])/1000000
					if "total_MPIIO_F_READ_TIME:" in line and not line.startswith("#"):
						readTimeMPIIO = float(line.split(" ")[1])
						readTimeMPIIOonly = readTimeMPIIO
					if "total_MPIIO_F_MAX_READ_TIME:" in line and not line.startswith("#"):
						slowReadTimeMPIIO = float(line.split(" ")[1])

					#jiwoo
					#MPIIO metadata request
					if "total_MPIIO_INDEP_OPENS:" in line and not line.startswith("#"):
						IndOpenReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_COLL_OPENS:" in line and not line.startswith("#"):
						ColOpenReqMPIIO = float(line.split(" ")[1])

					#MPIIO I/O request
					if "total_MPIIO_INDEP_WRITES:" in line and not line.startswith("#"):
						IndWriteReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_COLL_WRITES:" in line and not line.startswith("#"):
						ColWriteReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_INDEP_READS:" in line and not line.startswith("#"):
						IndReadReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_COLL_READS:" in line and not line.startswith("#"):
						ColReadReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_SPLIT_READS:" in line and not line.startswith("#"):
						SplitReadReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_SPLIT_WRITES:" in line and not line.startswith("#"):
						SplitWriteReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_NB_READS:" in line and not line.startswith("#"):
						NbReadReqMPIIO = float(line.split(" ")[1])
					if "total_MPIIO_NB_WRITES:" in line and not line.startswith("#"):
						NbWriteReqMPIIO = float(line.split(" ")[1])

					#STDIO I/O Rate
					#writeRateSTDIO
					if "total_STDIO_BYTES_WRITTEN:" in line and not line.startswith("#"):
						writeBytesSTDIO = float(line.split(" ")[1])/1000000
					if "total_STDIO_F_WRITE_TIME:" in line and not line.startswith("#"):
						writeTimeSTDIO = float(line.split(" ")[1])
						writeTimeSTDIOonly = writeTimeSTDIO
					#readRateSTDIO
					if "total_STDIO_BYTES_READ:" in line and not line.startswith("#"):
						readBytesSTDIO = float(line.split(" ")[1])/1000000
					if "total_STDIO_F_READ_TIME:" in line and not line.startswith("#"):
						readTimeSTDIO = float(line.split(" ")[1])
						readTimeSTDIOonly = readTimeSTDIO

					#jiwoo
					#STDIO I/O request
					if "total_STDIO_OPENS:" in line and not line.startswith("#"):
						OpenReqSTDIO = float(line.split(" ")[1])
					if "total_STDIO_WRITES:" in line and not line.startswith("#"):
						WriteReqSTDIO = float(line.split(" ")[1])
					if "total_STDIO_READS:" in line and not line.startswith("#"):
						ReadReqSTDIO = float(line.split(" ")[1])
					if "total_STDIO_SEEKS:" in line and not line.startswith("#"):
						SeekReqSTDIO = float(line.split(" ")[1])
					if "total_STDIO_FLUSHES:" in line and not line.startswith("#"):
						FlushReqSTDIO = float(line.split(" ")[1])

					#POSIX seq request
					if "total_POSIX_WRITES:" in line and not line.startswith("#"):
						WriteReqPOSIX = float(line.split(" ")[1])
					if "total_POSIX_SEQ_WRITES:" in line and not line.startswith("#"):
						seqWriteReqPOSIX = float(line.split(" ")[1])
					if 'total_POSIX_CONSEC_WRITES:' in line and not line.startswith('#'):
						consecWriteReqPOSIX = float(line.split(' ')[1])

					if "total_POSIX_READS" in line and not line.startswith("#"):
						ReadReqPOSIX = float(line.split(" ")[1])
					if "total_POSIX_SEQ_READS:" in line and not line.startswith("#"):
						seqReadReqPOSIX = float(line.split(" ")[1])
					if 'total_POSIX_CONSEC_READS:' in line and not line.startswith('#'):
						consecReadReqPOSIX = float(line.split(' ')[1])

					#POSIX metadata request
					if "total_POSIX_OPENS:" in line and not line.startswith("#"):
						OpenReqPOSIX = float(line.split(" ")[1])
					if "total_POSIX_SEEKS:" in line and not line.startswith("#"):
						SeekReqPOSIX = float(line.split(" ")[1])
					if "total_POSIX_STATS:" in line and not line.startswith("#"):
						StatReqPOSIX = float(line.split(" ")[1])
					if "# agg_perf_by_slowest" in line and line.startswith("#"):
						writeRateTotal = float(line.split(" ")[2])
					#byfile?
					if "agg_perf_by_slowest:" in line and line.startswith("#"):
						if writeRatePOSIX == 0:
							writeRatePOSIX = float(line.split(" ")[2])
						elif writeRateMPIIO == 0:
							writeRateMPIIO = float(line.split(" ")[2])
						else:
							writeRateSTDIO = float(line.split(" ")[2])

			#if(writeBytesPOSIX == 0):
			#	   return

			#END LINE loop
			ReadReqMPIIO = IndReadReqMPIIO + ColReadReqMPIIO
			WriteReqMPIIO = IndWriteReqMPIIO + ColWriteReqMPIIO
			OpenReqMPIIO = IndOpenReqMPIIO + ColOpenReqMPIIO

			writeTimePOSIX = metaTimePOSIX + writeTimePOSIX
			readTimePOSIX = metaTimePOSIX + readTimePOSIX

			writeTimeMPIIO = metaTimeMPIIO + writeTimeMPIIO
			readTimeMPIIO = metaTimeMPIIO + writeTimeMPIIO

			writeTimeSTDIO = metaTimeSTDIO + writeTimeSTDIO
			readTimeSTDIO = metaTimeSTDIO + writeTimeSTDIO

			#calculate rates after file iteration
			#MB/s

			if writeBytesPOSIX != 0 and writeRatePOSIX == 0:
				writeRatePOSIX = float(writeBytesPOSIX/writeTimePOSIX)
			if readBytesPOSIX != 0:
				readRatePOSIX = readBytesPOSIX/readTimePOSIX
			if writeBytesMPIIO != 0 and writeRateMPIIO == 0:
				writeRateMPIIO = writeBytesMPIIO/writeTimeMPIIO
			if readBytesMPIIO != 0:
				readRateMPIIO = readBytesMPIIO/readTimeMPIIO
			if writeBytesSTDIO != 0 and writeRateSTDIO == 0:
				writeRateSTDIO = writeBytesSTDIO/writeTimeSTDIO
			if readBytesSTDIO != 0:
				readRateSTDIO = readBytesSTDIO/readTimeSTDIO
			#calculate total read/write btyes
			#MB/s
			writeBytesTotal = float(writeBytesPOSIX + writeBytesMPIIO + writeBytesSTDIO)
			#1GB
			#jiwoo
			#if(writeBytesTotal < 1000):
			#	return
			readBytesTotal = float(readBytesPOSIX + readBytesMPIIO + readBytesSTDIO)
			writeRateTotal = (writeRatePOSIX + writeRateMPIIO + writeRateSTDIO)
			#readRateTotal = (readRatePOSIX + readRateMPIIO + readRateSTDIO)
			if WriteReqPOSIX != 0:
				seqWritePct = (seqWriteReqPOSIX/WriteReqPOSIX) * 100
				consecWritePct = consecWriteReqPOSIX / WriteReqPOSIX * 100
			if ReadReqPOSIX != 0:
				seqReadPct = (seqReadReqPOSIX/ReadReqPOSIX) * 100
				consecReadPct = consecReadReqPOSIX / ReadReqPOSIX * 100
			#in some cases consecWrite or seqWrite can be over 100, higher than num write request
			#just set them as 100
			if consecWritePct > 100:
				consecWritePct = 100
			if seqWritePct > 100:
				seqWritePct = 100
			if consecReadPct > 100:
				consecReadPct = 100
			if seqReadPct > 100:
				seqReadPct = 100
			totalWriteTemp = writeLess1m + writeMore1m
			if totalWriteTemp != 0:
				writeLess1m = (writeLess1m/totalWriteTemp) * 100
				writeLess1k = (writeLess1k/totalWriteTemp) * 100
				writeMore1m = (writeMore1m/totalWriteTemp) * 100
				writeMore1k = (writeMore1k/totalWriteTemp) * 100
			totalReadTemp = readLess1m + readMore1m
			if totalReadTemp != 0:
				readLess1m = (readLess1m/totalReadTemp) * 100
				readLess1k = (readLess1k/totalReadTemp) * 100
				readMore1m = (readMore1m/totalReadTemp) * 100
				readMore1k = (readMore1k/totalReadTemp) * 100
			try:
				endTime = startTime + datetime.timedelta(seconds=int(runTime))
			except:
				endTime = "-1"
			#In Case of no POSIX, just set start IO time to 0
			if ioStartTime == -1:
				ioStartTime = 0
			totalFile = int(totalFilePOSIX) + int(totalFileMPIIO) + int(totalFileSTDIO)
			totalIOReqPOSIX = int(ReadReqPOSIX) + int(WriteReqPOSIX)
			totalMetaReqPOSIX = int(OpenReqPOSIX) + int(SeekReqPOSIX) + int(StatReqPOSIX)
			totalIOReqMPIIO = int(ReadReqMPIIO) + int(WriteReqMPIIO)
			totalMetaMPIIO = int(OpenReqMPIIO)
			totalIOReqSTDIO = int(ReadReqSTDIO) + int(WriteReqSTDIO)
			totalMetaSTDIO = int(OpenReqSTDIO) + int(SeekReqSTDIO)
	#-----------------------------------------------------------------------darhsna
			#get numCPU from SLURM
			#figure out which th darshanfile with same jobID (to match SLURM)
			numCPU = -1
			numNode = -1
			numOST = -1
			usedOSTName = []
			#ostlist = "NOTHING" # if nothing is written to ostlist
			if usedOST != []:
				numOST = 0
				try:
					for ost in usedOST:
						numOST = numOST + 1
						ostlist = ostlist + " " + str(ost)
					ostlist = ostlist[1:]
					ostlist = "%s" % ostlist
				except:
					pass
			else:
				ostlist = "-1"
			#print("----")
			#print(numOST)
			#print(ostlist)

			# --- LMT START
			mdsCPUMean = mdsCPU95 = ossReadMean = ossRead95 = ossWriteMean = ossWrite95 = 0
			ossReadHigher1g = ossReadHigher4g = ossWriteHigher1g = ossWriteHigher4g = 0
			ossWriteLargest = ossReadLargest = 0
			ossWriteStart95 = ossWriteStartLargest = 0

			
			###
			###		if run time is too short, there might be no data to read
			###

			#LMT: START - END TIME variable

			lmtStartTime = startTime
			lmtEndTime = startTime + datetime.timedelta(seconds=int(runTime))

			# VAR - ossWriteStart95
			
			df = ossWrite_df.loc[str(lmtStartTime-datetime.timedelta(seconds=int(5))):str(lmtEndTime)]
			df = df.sum(axis = 1)
			i=0
			for item in df.describe([.95]):
				if i ==5:
					if math.isnan(item):
						ossWriteStart95 = "-1"
					else:
						ossWriteStart95 = item
				i = i+1
		
			# VAR - mdsCPUMean , mdsCPU95
			df = mdsCPU_df.loc[str(lmtStartTime):str(lmtEndTime)]
			df = df.sum(axis=1)
			i=0
			for item in df.describe([.95]):
				if i == 1:
					if math.isnan(item):
						mdsCPUMean = "-1"
					else:
						mdsCPUMean = item
				elif i == 5:
					if math.isnan(item):
						mdsCPU95 = "-1"
					else:
						mdsCPU95 = item
				i = i+1

			# VAR - ossReadLargest, ossReadHigher1g-4g
			df = ossRead_df.loc[str(lmtStartTime):str(lmtEndTime)]
			dfUsed = df

			for column in df:
				if df[column].mean() > ossReadLargest:
					ossReadLargest = df[column].mean()
					if df[column].mean() > 1000000000:
						ossReadHigher1g = ossReadHigher1g + 1
					if df[column].mean() > 4000000000:
						ossReadHigher4g = ossReadHigher4g + 1


			# VAR - ossReadMean, ossRead95
			df = df.sum(axis=1)
			i=0
			for item in df.describe([.95]):
				if i == 1:
					if math.isnan(item):
						ossReadMean = "-1"
					else:
						ossReadMean = item
				elif i == 5:
					if math.isnan(item):
						ossRead95 = "-1"
					else:
						ossRead95 = item
				i = i+1
			
			# VAR - ossWriteLargest, ossWriteHigher1g-4g
			df = ossWrite_df.loc[str(lmtStartTime):str(lmtEndTime)]
			dfUsed = df
			for column in df:
				if df[column].mean() > ossWriteLargest:
					ossWriteLargest = df[column].mean()
					if df[column].mean() > 1000000000:
						ossWriteHigher1g = ossWriteHigher1g + 1
					if df[column].mean() > 4000000000:
						ossWriteHigher4g = ossWriteHigher4g + 1


			###
			### writeBytesPOSIX is originally in MB/s so change it to GB/s
			### because df is in Gib/s!
			###
			
			# VAR - ossWriteMean, ossWrite95
			writeBytesPct = writeBytesPOSIX/1000
			df = df.sum(axis=1)

			if df.agg('sum') != 0:
				writeBytesPct = (writeBytesPct * 100)/df.agg('sum')
			else:
				writeBytesPct = -1
			i=0
			for item in df.describe([.95]):
				if i == 1:
					if math.isnan(item):
						ossWriteMean = "-1"
					else:
						ossWriteMean = item
				elif i == 5:
					if math.isnan(item):
						ossWrite95 = "-1"
					else:
						ossWrite95 = item
				i = i+1
			i=0
			

			#print("DK: printing vars\n") # 6th to the last is for ostlist and it is text type. Do something when writing python int list
			'''print("'%s','%s','%s',%s,'%s','%s',%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, "\
				"%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, "\
				"%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, "\
				"%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s, '%s',%s,%s,%s,%s,%s" % (file,
					progName, userID, jobID, startTime, endTime, int(ioStartTime), runTime, numProc, numCPU, numNode,
					numOST, stripeSize, isknl,
					totalFile, totalIOReqPOSIX, totalMetaReqPOSIX,
					totalIOReqMPIIO, totalMetaReqMPIIO, totalIOReqSTDIO, totalMetaReqSTDIO,
					mdsCPUMean, mdsCPU95, mdsOPSMean, mdsOPSMin, mdsOPS95, ossReadMean, ossRead95, ossWriteMean, ossWrite95,
					ossReadMeanUsed, ossRead95Used, ossWriteMeanUsed, ossWrite95Used, ossWriteLargestUsed, ossReadLargestUsed,
					ossReadHigher1g, ossReadHigher4g, ossWriteHigher1g, ossWriteHigher4g,
					totalFilePOSIX,totalFileMPIIO,totalFileSTDIO,seqWritePct,seqReadPct,consecWritePct,consecReadPct,
					writeLess1k, writeMore1k, readLess1k, readMore1k,
					writeLess1m, writeMore1m, readLess1m, readMore1m,
					writeBytesTotal, readBytesTotal, writeRateTotal, readRateTotal,
					writeBytesPOSIX, writeTimePOSIX, writeRatePOSIX, readBytesPOSIX, readTimePOSIX, readRatePOSIX,
					writeBytesMPIIO,writeTimeMPIIO,writeRateMPIIO,readBytesMPIIO,readTimeMPIIO,readRateMPIIO,
					writeBytesSTDIO,writeTimeSTDIO,writeRateSTDIO,readBytesSTDIO,readTimeSTDIO,readRateSTDIO,
					ReadReqPOSIX, WriteReqPOSIX, OpenReqPOSIX, SeekReqPOSIX, StatReqPOSIX,
					readTimePOSIXonly, writeTimePOSIXonly, readTimeMPIIOonly, writeTimeMPIIOonly, readTimeSTDIOonly, writeTimeSTDIOonly, metaTimePOSIX, metaTimeMPIIO, metaTimeSTDIO,
					slowWriteTimePOSIX, slowReadTimePOSIX, slowWriteTimeMPIIO, slowReadTimeMPIIO, IndOpenReqMPIIO, ColOpenReqMPIIO, IndWriteReqMPIIO, ColWriteReqMPIIO, IndReadReqMPIIO, ColReadReqMPIIO,
					SplitReadReqMPIIO, SplitWriteReqMPIIO, NbReadReqMPIIO, NbWriteReqMPIIO,
					OpenReqMPIIO, ReadReqMPIIO, WriteReqMPIIO,
					OpenReqSTDIO, ReadReqSTDIO, WriteReqSTDIO, SeekReqSTDIO, FlushReqSTDIO,
					ostlist, ossWriteLargest, ossReadLargest, ossWriteStart95 ,ossWriteStartLargest,writeBytesPct))
'''

			insertString = "INSERT INTO " + app_sqlite + " VALUES ('%s','%s','%s',%s,'%s','%s',%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s, '%s',%s,%s,%s,%s,%s)" %(file,
				progName, userID, jobID, startTime, endTime, int(ioStartTime), runTime, numProc, numCPU, numNode,
				numOST, stripeSize, isknl,
				totalFile, totalIOReqPOSIX, totalMetaReqPOSIX,
				totalIOReqMPIIO, totalMetaReqMPIIO, totalIOReqSTDIO, totalMetaReqSTDIO,
				mdsCPUMean, mdsCPU95, ossReadMean, ossRead95, ossWriteMean, ossWrite95,
				ossReadHigher1g, ossReadHigher4g, ossWriteHigher1g, ossWriteHigher4g,
				totalFilePOSIX,totalFileMPIIO,totalFileSTDIO,seqWritePct,seqReadPct,consecWritePct,consecReadPct,
				writeLess1k, writeMore1k, readLess1k, readMore1k,
				writeLess1m, writeMore1m, readLess1m, readMore1m,
				writeBytesTotal, readBytesTotal, writeRateTotal, readRateTotal,
				writeBytesPOSIX, writeTimePOSIX, writeRatePOSIX, readBytesPOSIX, readTimePOSIX, readRatePOSIX,
				writeBytesMPIIO,writeTimeMPIIO,writeRateMPIIO,readBytesMPIIO,readTimeMPIIO,readRateMPIIO,
				writeBytesSTDIO,writeTimeSTDIO,writeRateSTDIO,readBytesSTDIO,readTimeSTDIO,readRateSTDIO,
				ReadReqPOSIX, WriteReqPOSIX, OpenReqPOSIX, SeekReqPOSIX, StatReqPOSIX,
				readTimePOSIXonly, writeTimePOSIXonly, readTimeMPIIOonly, writeTimeMPIIOonly, readTimeSTDIOonly, writeTimeSTDIOonly, metaTimePOSIX, metaTimeMPIIO, metaTimeSTDIO,
				slowWriteTimePOSIX, slowReadTimePOSIX, slowWriteTimeMPIIO, slowReadTimeMPIIO, IndOpenReqMPIIO, ColOpenReqMPIIO, IndWriteReqMPIIO, ColWriteReqMPIIO, IndReadReqMPIIO, ColReadReqMPIIO,
				SplitReadReqMPIIO, SplitWriteReqMPIIO, NbReadReqMPIIO, NbWriteReqMPIIO,
				OpenReqMPIIO, ReadReqMPIIO, WriteReqMPIIO,
				OpenReqSTDIO, ReadReqSTDIO, WriteReqSTDIO, SeekReqSTDIO, FlushReqSTDIO,
				ostlist, ossWriteLargest, ossReadLargest, ossWriteStart95 ,ossWriteStartLargest,writeBytesPct)
			#print(insertString)
			return insertString

#main
#output = open(outputdir, 'wb+', 1)
#output = open(outputdir, 'w+', 1, 'utf-8')
output = open(outputdir, 'w+', 1, encoding='utf-8')
main_start = datetime.datetime.now()
#print(main_start)

# LMT database load from csv files
lmtpath = "/scratch/snuteam/perftest/log/LMT/"
lmtdate = "2021-08-09"
# MDS
mdsCPU_df = pandas.read_csv(lmtpath+lmtdate+"_MDS_D.csv")[['TIMESTAMP','PCT_CPU']]
mdsCPU_df = mdsCPU_df.set_index("TIMESTAMP")

# OST - Write
ossWrite_df = pandas.read_csv(lmtpath+lmtdate+"_OST_D.csv")[['TIMESTAMP','WRITE_BYTES']]
ossWrite_df = ossWrite_df.groupby('TIMESTAMP').agg({'WRITE_BYTES':sum})

ossWrite_df['WRITE_BYTESD'] = ossWrite_df['WRITE_BYTES'].diff(1).fillna(0.0)
ossWrite_df = ossWrite_df.drop(['WRITE_BYTES'], axis = 1)

#OST - Read
ossRead_df = pandas.read_csv(lmtpath+lmtdate+"_OST_D.csv")[['TIMESTAMP','READ_BYTES']]
ossRead_df = ossRead_df.groupby('TIMESTAMP').agg({'READ_BYTES':sum})

ossRead_df['READ_BYTESD'] = ossRead_df['READ_BYTES'].diff(1).fillna(0.0)
ossRead_df = ossRead_df.drop(['READ_BYTES'], axis = 1)

#print("LMT load status: mdsCPU_df-"+str(mdsCPU_df.shape)+", ossWrite/Read_df-"+str(ossWrite_df.shape))

#insertSql = process(darshandir)
#c.execute(insertSql)
#print()
#print(insertSql)

tempList = []
for logfile in os.scandir(logdir):
	lf = (os.path.basename(logfile.path))

	tempList.append(logfile.path)
print("total file count: " + str(len(tempList)))

p = multiprocessing.Pool(32)
result = p.map_async(process, tempList, chunksize=1)

while not result.ready():
	time.sleep(1)
print ("while complete")

try:
	real_result = result.get()
	p.close()
	p.join()
	print ("real_result len: " +str(len(real_result)))
	for item in real_result:
		if str(item) != "None":
			output.write((str(item) + '\n'))

except Exception as e:
	print("real_result error")
	print(e)
	timer = 0
	while result._number_left != 0:
		timer = timer + 1
		time.sleep(1)
		if timer > 1:
			print ("no progress, break")
			p.terminate()
			with open("skipped.txt", "a") as error:
				error.write("Error on :" + str(year) + " "+ str(month) + " " + str(day) + "\n")
			break

		print ("Finish processing for :" + str(year) + " "+ str(month) + " " + str(day))
		print (datetime.datetime.now() - main_start)
		

print("END processing ")
print (datetime.datetime.now() - main_start)
#SQLite apply
for line in open(outputdir, 'r', 1):
	#print(line)
	c.execute(line)

conn.commit()
conn.close()

print("END all sql execute")
