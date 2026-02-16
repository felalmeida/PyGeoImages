#!/usr/bin/python3
# -*- coding: UTF-8 -*-
###############################################################################
# Module:   PyGeoImages.py          Autor: Felipe Almeida                     #
# Start:    05-Feb-2026             LastUpdate: 15-Feb-2026     Version: 1.0  #
###############################################################################

import sys
import os
import datetime
import json
import pystac_client
import planetary_computer
import geojson
import turfpy.measurement
import hashlib

ThisPath    = os.path.dirname(__file__)+'/'
ConfigPath  = ThisPath+'config/'
MetaPath    = ThisPath+'meta/'
LogPath     = ThisPath+'log/'
FieldDelim  = ','

if not os.path.exists(MetaPath): os.makedirs(MetaPath)
if not os.path.exists(LogPath): os.makedirs(LogPath)

ExecutionId = ''
ExecutionDt = ''
jSources = {}
gStatesInterestBBOX = []
gCitiesInterestBBOX = []


def DictArrayToCsv(v_jArray, v_FieldDelim=','):
    CsvHeader = v_jArray[0].keys()
    CsvHeader = [Field for Field in CsvHeader if Field[0] != '_']
    CsvHeaderStr = v_FieldDelim.join(CsvHeader)+'\n'

    CsvBody = ''
    for jItem in v_jArray:
        CsvLine = ''
        for Field in CsvHeader:
            if Field in jItem.keys():
                CsvLine += str(jItem[Field]) + v_FieldDelim
        CsvLine = CsvLine[:-1]
        CsvLine += '\n'
        CsvBody += CsvLine

    return CsvHeaderStr+CsvBody


def EnvironmentSetup():
    global ConfigPath, jSources, gStatesInterestBBOX, gCitiesInterestBBOX

    ### Sources Config
    with open(ConfigPath+'Sources.json', 'r') as fConfigFile:
        jSources = json.load(fConfigFile)
    if (len(jSources) > 0):
        jSources = {key:val for key,val in jSources.items() if val['Enabled'] == True}

    ### Enabled Brazilian States
    jStates = []
    with open(ConfigPath+'Estados_GeoJS.json', 'r') as fConfigFile:
        jStatesGeoJS = json.load(fConfigFile)
        jStatesGeoJS = jStatesGeoJS['features']

    with open(ConfigPath+'Estados.json', 'r') as fConfigFile:
        jStatesAll = json.load(fConfigFile)

    for jState in jStatesAll:
        if (jState['Enabled']):
            jState['features'] = []
            for StateGeo in jStatesGeoJS:
                if (StateGeo['id'] == jState['Sigla']):
                    jState['features'].append(StateGeo)
                    break
            jStates.append(jState)
    del jStatesAll
    del jStatesGeoJS

    ### Enabled Brazilian Cities
    jCities = []
    with open(ConfigPath+'Municipios_GeoJS.json', 'r') as fConfigFile:
        jCitiesGeoJS = json.load(fConfigFile)
        jCitiesGeoJS = jCitiesGeoJS['features']

    with open(ConfigPath+'Municipios.json', 'r') as fConfigFile:
        jCitiesAll = json.load(fConfigFile)

    for jCity in jCitiesAll:
        if (jCity['Enabled']):
            jCity['features'] = []
            for CityGeo in jCitiesGeoJS:
                if (int(CityGeo['properties']['id']) == int(jCity['Cod_Municipio_Completo'])):
                    jCity['features'].append(CityGeo)
                    break
            jCities.append(jCity)
    del jCitiesAll
    del jCitiesGeoJS

    ### Interests Areas For States
    gStatesInterestArea = []
    for itState in jStates:
        gStatesInterestArea.append(geojson.loads(json.dumps({'type':'FeatureCollection','features':itState['features']})))
        gStatesInterestBBOX.append({'id':itState['Sigla'],'name':itState['Estado'],'bbox':turfpy.measurement.bbox(gStatesInterestArea[-1])})
    del gStatesInterestArea

    ### Interests Areas For Cities
    gCitiesInterestArea = []
    for itCity in jCities:
        gCitiesInterestArea.append(geojson.loads(json.dumps({'type':'FeatureCollection','features':itCity['features']})))
        gCitiesInterestBBOX.append({'id':itCity['Cod_Municipio_Completo'],'name':itCity['Nome_Municipio'],'bbox':turfpy.measurement.bbox(gCitiesInterestArea[-1])})
    del gCitiesInterestArea


def PlanetaryComputer(v_Source=None, v_dtLoopStart=None, v_dtLoopEnd=None, v_bUpdateCatallog=False):
    global ExecutionId, ExecutionDt, MetaPath, LogPath, jSources, gCitiesInterestBBOX, FieldDelim

    SourceData = jSources[v_Source]
    MetaFileName = os.path.realpath(MetaPath+SourceData['SysName']+'_'+'Collections.meta.json')
    dtRangeStr = v_dtLoopStart.astimezone().isoformat()+'/'+v_dtLoopEnd.astimezone().isoformat()

    planetarycomputer_catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace)

    if (v_bUpdateCatallog):
        ### Organize Collections in Meta File Keeping Enable Status
        ArrCollections = []
        jCollections = []

        if os.path.exists(MetaFileName):
            with open(MetaFileName, 'r') as fConfigFile:
                jCollections = json.load(fConfigFile)

        for collection in list(planetarycomputer_catalog.get_collections()):
            DctCollection = collection.to_dict()
            ItEnabled = True
            LocalData = None
            for Collection in jCollections:
                if (Collection['CollectionId'] == DctCollection['id']):
                    LocalData = Collection
                    break
            if (LocalData is not None):
                if ('Enabled' in LocalData):
                    ItEnabled = LocalData['Enabled']

            jCollection = {
                'Enabled':ItEnabled,
                '_dt_update':datetime.datetime.now(datetime.UTC).astimezone().isoformat(),
                '_ts_update':int(datetime.datetime.now(datetime.UTC).timestamp()),
                '_id':DctCollection['id'],
                'Source':v_Source,
                'CollectionId':DctCollection['id'],
                'Title':DctCollection['title'],
                'Type':DctCollection['type'],
                'StacVersion':DctCollection['stac_version']
            }

            ArrCollections.append(jCollection)
        ArrCollections.sort(key=lambda itItem: itItem["CollectionId"])
        with open(MetaFileName,'w') as fConfigFile:
            fConfigFile.write(json.dumps(ArrCollections,sort_keys=True,indent=4))
        del ArrCollections
        del jCollections

    ### Get Updated and Enabled Collections
    jCollections = []
    with open(MetaFileName, 'r') as fConfigFile:
        for Collections in json.load(fConfigFile):
            if (Collections['Enabled'] == True):
                jCollections.append(Collections)

    ### Get Metadata for Selected Dates, Collections and Interests BBOX
    LogDataArr = []
    for collection in jCollections:
        CollectionId = collection['CollectionId']
        for gInterestBBOX in gCitiesInterestBBOX:
            CatSearch = planetarycomputer_catalog.search(collections=[CollectionId], bbox=gInterestBBOX['bbox'], datetime=dtRangeStr)
            for CatSearchItem in CatSearch.items_as_dicts():
                CatSearchItem['_id'] = CatSearchItem['id']
                CatSearchItem['_dt_update'] = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
                CatSearchItem['_ts_update'] = int(datetime.datetime.now(datetime.UTC).timestamp())
                CatSearchItem['_query'] = {
                    'collection':CollectionId,
                    'InterestBBOX_id':gInterestBBOX['id'],
                    'InterestBBOX_name':gInterestBBOX['name'],
                    'datetime':dtRangeStr
                }
                dtItem = datetime.datetime.fromisoformat(CatSearchItem['properties']['datetime'])
                SavePath = os.path.realpath(MetaPath+CollectionId+'/'+dtItem.strftime("%Y%m%d")+'/'+str(gInterestBBOX['id']))
                FileName = SavePath+'/'+CatSearchItem['id']+'.json'

                ### Search For Duplicated Files
                bFileExists = False
                ActualFileName = FileName
                for root, dirs, files in os.walk(os.path.realpath(MetaPath+CollectionId+'/'+dtItem.strftime("%Y%m%d")+'/')):
                    if CatSearchItem['id']+'.json' in files:
                        bFileExists = True
                        ActualFileName = os.path.join(root, CatSearchItem['id']+'.json')
                CatSearchItem['_filename'] = ActualFileName

                ### Save Reference Log to Array
                jLogData = {
                    'ExecutionId':ExecutionId,
                    'ExecutionDt':ExecutionDt,
                    'CollectionId':CollectionId,
                    'InterestBBOXId':gInterestBBOX['id'],
                    'InterestBBOXName':gInterestBBOX['name'],
                    'dtRangeStr':dtRangeStr,
                    'dtItem':dtItem.isoformat(),
                    'FileName':CatSearchItem['_filename']
                }
                LogDataArr.append(jLogData)
                print(json.dumps(jLogData,indent=4,sort_keys=True))

                ### Save File Locally
                if (not bFileExists):
                    os.makedirs(SavePath, exist_ok=True)
                    with open(FileName,'w') as fConfigFile:
                        fConfigFile.write(json.dumps(CatSearchItem,sort_keys=True,indent=4))

    ### Save Log File (as CSV)
    with open(os.path.realpath(LogPath+ExecutionId+'.csv'),'w') as fCsvLogFile:
        fCsvLogFile.write(DictArrayToCsv(LogDataArr, FieldDelim))


def MainProcess():
    global ExecutionId, ExecutionDt, jSources

    ExecutionDt = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
    ExecutionId = str(hashlib.md5((ExecutionDt).encode('UTF-8')).hexdigest())

    EnvironmentSetup()

    dtLoopEnd   = datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    dtLoopStart = (dtLoopEnd.replace(hour=0, minute=0, second=0, day=1, month=1) - datetime.timedelta(days=1)).replace(day=1, month=1) # Get First Day of past Year

    # Just for DEV Tests
    dtLoopStart = dtLoopEnd.replace(hour=0, minute=0, second=0) - datetime.timedelta(days=7)

    for Source in jSources:
        if (jSources[Source]['SysName'] == 'PlanetaryComputer'):
            PlanetaryComputer(Source, dtLoopStart, dtLoopEnd, False)


def main():
    try:
        MainProcess()
        sys.exit(0)
    except KeyboardInterrupt:
        print("Py Geo Images Interrupted!")
        sys.exit(1)


if __name__ == "__main__":
    main()
