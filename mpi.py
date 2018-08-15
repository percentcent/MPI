from mpi4py import MPI
import numpy as np
import json as json
import time


def rankDict(dictionary):
	tupleList = []
	for item in dictionary:
		value = dictionary[item]
		temp = (item,value)
		tupleList.append(temp)
	tupleList = sorted(tupleList, key=lambda item:item[1])
	tupleList.reverse()
	return tupleList

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank > 0:
	grid = open('melbGrid.json','r',encoding="utf-8")
	gridInfo = json.load(grid)

	feature = gridInfo["features"]

	gridBox = []
	lngMinAll = 150
	lngMaxAll = 140
	latMaxAll = -38
	latMinAll = -37
	for i in range(len(feature)):
		id = feature[i]["properties"]["id"]
		lngMin = feature[i]["properties"]["xmin"]
		lngMax = feature[i]["properties"]["xmax"]
		latMin = feature[i]["properties"]["ymin"]
		latMax = feature[i]["properties"]["ymax"]
		if(lngMin < lngMinAll):
			lngMinAll = lngMin
		if(lngMax > lngMaxAll):
			lngMaxAll = lngMax
		if(latMin < latMinAll):
			latMinAll = latMin
		if(latMax > latMaxAll):
			latMaxAll = latMax

		dictI = {}
		dictI["id"] = id
		dictI["lngMin"] = lngMin
		dictI["lngMax"] = lngMax
		dictI["latMin"] = latMin
		dictI["latMax"] = latMax
		gridBox.append(dictI)

	gridCount = {}
	for grid in gridBox:
		id = grid["id"]
		gridCount[id] = 0

	while(True):
		data = comm.recv(source=0,tag=11)
		if(data == 'finish'):
			break
		tweet = json.loads(data)
		doc = tweet['doc']
		if('coordinates' in doc):
			coordinates = doc['coordinates']
			if('coordinates' in coordinates):
				if(len(coordinates['coordinates']) == 2):
					lat = coordinates['coordinates'][0]
					lng = coordinates['coordinates'][1]
					if(lat is None or lng is None):
						continue
					if(lat >= latMinAll and lat <= latMaxAll and lng <= lngMaxAll and lng >= lngMinAll):
						for grid in gridBox:
							if(lat >= grid["latMin"] and lat <= grid["latMax"] and lng <= grid["lngMax"] and lng >= grid["lngMin"]):
								gridCount[grid["id"]] += 1
								break
	send_result = gridCount
	recv_result = comm.gather(send_result, root=0)

if rank == 0:
	start = time.clock()
	count = 0
	coreNum = 0
	for line in open('bigInstagram.json','r',encoding="utf-8"):
		if(count == 0):
			count = 1
			continue
		count += 1
	
		if(count == 3245342):
			break
		'''
		if(count == 3245341):
			line = line[:-1]
		else:
			line = line[:-2]
		'''
		index = line.rfind('}')+1
		line1 = line[0:index]
		data = line1
		coreNum += 1
		if(coreNum <= 7):
			comm.send(data, dest=coreNum, tag = 11)
		else:
			coreNum = 1
			comm.send(data, dest=coreNum, tag = 11)
	data = 'finish'
	for i in range(7):
		comm.isend(data,dest = i+1, tag =11)

	send_result = {}
	recv_result = comm.gather(send_result, root=0)

	gridCount = {}
	gridCount['A1'] = 0
	gridCount['A2'] = 0
	gridCount['A3'] = 0
	gridCount['A4'] = 0
	gridCount['B1'] = 0
	gridCount['B2'] = 0
	gridCount['B3'] = 0
	gridCount['B4'] = 0
	gridCount['C1'] = 0
	gridCount['C2'] = 0
	gridCount['C3'] = 0
	gridCount['C4'] = 0
	gridCount['C5'] = 0
	gridCount['D3'] = 0
	gridCount['D4'] = 0
	gridCount['D5'] = 0

	for i in range(7):
		result = recv_result[i+1]
		for key in result:
			gridCount[key] += result[key]

	gridRow = {}
	gridRow['A-Row'] = gridCount["A1"]+gridCount["A2"]+gridCount["A3"]+gridCount["A4"]
	gridRow['B-Row'] = gridCount["B1"]+gridCount["B2"]+gridCount["B3"]+gridCount["B4"]
	gridRow['C-Row'] = gridCount["C1"]+gridCount["C2"]+gridCount["C3"]+gridCount["C4"]+gridCount["C5"]
	gridRow['D-Row'] = gridCount["D3"]+gridCount["D4"]+gridCount["D5"]


	gridcolumn = {}
	gridcolumn['Column-1'] = gridCount["A1"]+gridCount["B1"]+gridCount["C1"]
	gridcolumn['Column-2'] = gridCount["A2"]+gridCount["B2"]+gridCount["C2"]
	gridcolumn['Column-3'] = gridCount["A3"]+gridCount["B3"]+gridCount["C3"]+gridCount["D3"]
	gridcolumn['Column-4'] = gridCount["A4"]+gridCount["B4"]+gridCount["C4"]+gridCount["D4"]
	gridcolumn['Column-5'] = gridCount["C5"]+gridCount["D5"]

	outputF = open('bigMpi.txt','w')

	rankGrid = rankDict(gridCount)
	for i in range(len(rankGrid)):
		outputF.write(rankGrid[i][0]+": "+str(rankGrid[i][1])+" posts"+"\n")

	rankRow = rankDict(gridRow)
	for i in range(len(rankRow)):
		outputF.write(rankRow[i][0]+": "+str(rankRow[i][1])+" posts"+"\n")

	rankColumn = rankDict(gridcolumn)
	for i in range(len(rankColumn)):
		outputF.write(rankColumn[i][0]+": "+str(rankColumn[i][1])+" posts"+"\n")

	runtime = time.clock() - start
	outputF.write("Time used: "+str(runtime))




