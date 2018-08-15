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

countDict = {}
for grid in gridBox:
	id = grid["id"]
	countDict[id] = 0

start = time.clock()
count = 0
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
	tweet = json.loads(line1)
	doc = tweet['doc']
	if('coordinates' in doc):
		coordinates = doc['coordinates']
		if('coordinates' in coordinates):
			if(len(coordinates['coordinates']) > 0):
				lat = coordinates['coordinates'][0]
				lng = coordinates['coordinates'][1]
				if(lat is None or lng is None):
						continue
				if(lat >= latMinAll and lat <= latMaxAll and lng <= lngMaxAll and lng >= lngMinAll):
					for grid in gridBox:
						if(lat >= grid["latMin"] and lat <= grid["latMax"] and lng <= grid["lngMax"] and lng >= grid["lngMin"]):
							countDict[grid["id"]] += 1
							break

gridRow = {}
gridRow['A-Row'] = countDict["A1"]+countDict["A2"]+countDict["A3"]+countDict["A4"]
gridRow['B-Row'] = countDict["B1"]+countDict["B2"]+countDict["B3"]+countDict["B4"]
gridRow['C-Row'] = countDict["C1"]+countDict["C2"]+countDict["C3"]+countDict["C4"]+countDict["C5"]
gridRow['D-Row'] = countDict["D3"]+countDict["D4"]+countDict["D5"]


gridcolumn = {}
gridcolumn['Column-1'] = countDict["A1"]+countDict["B1"]+countDict["C1"]
gridcolumn['Column-2'] = countDict["A2"]+countDict["B2"]+countDict["C2"]
gridcolumn['Column-3'] = countDict["A3"]+countDict["B3"]+countDict["C3"]+countDict["D3"]
gridcolumn['Column-4'] = countDict["A4"]+countDict["B4"]+countDict["C4"]+countDict["D4"]
gridcolumn['Column-5'] = countDict["C5"]+countDict["D5"]

outputF = open('bigMpi_core1.txt','w')
rankGrid = rankDict(countDict)
for i in range(len(rankGrid)):
	outputF.write(rankGrid[i][0]+": "+str(rankGrid[i][1])+" posts"+'\n')

rankRow = rankDict(gridRow)
for i in range(len(rankRow)):
	outputF.write(rankRow[i][0]+": "+str(rankRow[i][1])+" posts"+'\n')

rankColumn = rankDict(gridcolumn)
for i in range(len(rankColumn)):
	outputF.write(rankColumn[i][0]+": "+str(rankColumn[i][1])+" posts"+'\n')

runtime = time.clock() - start
outputF.write("Time used: "+str(runtime))





