import os
import multiprocessing
from multiprocessing import Manager, Process
import time

#dir = "/apps/applications/darshan/logs/2021"
dir = "/scratch/s5104a08/darshan_log"

def process(fullpath):
	command = "/apps/applications/darshan/19.0.5/3.2.1/darshan-util/darshan-parser --all " + fullpath + " > " + fullpath + ".all"
	os.system(command)
	return

tempList = []
for monthdir in os.scandir(dir):
	month = int(os.path.basename(monthdir.path))
	if month != 7 :
		continue
	for daydir in os.scandir(monthdir.path):
		day = int(os.path.basename(daydir.path))
		if day != 10 :
			continue
		for file in os.scandir(daydir.path):
			if file.path.endswith(".darshan"):
				file = os.path.basename(file)
				fullpath = dir+"/"+str(month)+"/"+str(day)+"/"+file
				if os.path.exists(fullpath + ".all") :
					continue
				else :
					tempList.append(fullpath)
		print("total file count : " + str(len(tempList)) + " / " + str(month) + "-" + str(day))
		p = multiprocessing.Pool(16)
		result = p.map_async(process, tempList, chunksize=1)
		while not result.ready() :
			time.sleep(1)
		print("while complete")
		try:
			p.close()
			p.join()
		except Exception as e:
			print("result error")
			print(e)
			timer = 0
			while result._number_left != 0:
				print("num left: {}".format(result._number_left))
				timer = timer + 1
				time.sleep(1)
				if timer > 1:
					print("no progress, break")
					p.terminate()
		print("Finish processing for :" + str(month) + "-" + str(day))
	print("END")

