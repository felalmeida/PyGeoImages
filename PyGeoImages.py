#!/usr/bin/python3
# -*- coding: UTF-8 -*-
###############################################################################
# Module:   PyGeoImages.py          Autor: Felipe Almeida                     #
# Start:    05-Feb-2026             LastUpdate: 05-Feb-2026     Version: 1.0  #
###############################################################################

import sys
import os
import datetime
import json
import pystac_client
import planetary_computer
import geojson
import turfpy.measurement
# import geoai

ThisPath    = os.path.dirname(__file__)+'/'
ConfigPath  = ThisPath+'config/'
MetaPath    = ThisPath+'meta/'
DataPath    = ThisPath+'data/'

if not os.path.exists(MetaPath): os.makedirs(MetaPath)
if not os.path.exists(DataPath): os.makedirs(DataPath)

### Enabled Brazilian States
with open(ConfigPath+'Estados.json', 'r') as fConfigFile:
    jEstados = json.load(fConfigFile)
if (len(jEstados) > 0):
    jEstados = {key:val for key,val in jEstados.items() if val['Enabled'] == True}
EstadosIds = sorted([jEstados[Item]['Id'] for Item in jEstados])

### Enabled Brazilian States GeoJson
jBrasil = {'type':'FeatureCollection','features':[]}
with open(ConfigPath+'brazil_geo.json', 'r') as fConfigFile:
    jBrasilAll = json.load(fConfigFile)
for MapaEstado in jBrasilAll['features']:
    if (MapaEstado['id'] in EstadosIds):
        jBrasil['features'].append(MapaEstado)

gInterestArea = geojson.loads(json.dumps(jBrasil))
gInterestBBOX = turfpy.measurement.bbox(gInterestArea)

### Sources Config
with open(ConfigPath+'Sources.json', 'r') as fConfigFile:
    jSources = json.load(fConfigFile)
if (len(jSources) > 0):
    jSources = {key:val for key,val in jSources.items() if val['Enabled'] == True}


def PlanetaryComputer(v_Source=None, v_dtLoopStart=None, v_dtLoopEnd=None, v_bUpdateCatallog=False):
    global MetaPath, DataPath, jSources, gInterestArea, gInterestBBOX

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

    for collection in jCollections:
        CollectionId = collection['CollectionId']
        CatSearch = planetarycomputer_catalog.search(collections=[CollectionId], bbox=gInterestBBOX, datetime=dtRangeStr)
        CatItems = CatSearch.item_collection()
        # with open(DataPath+CollectionId+'.json','w') as fConfigFile:
        #     fConfigFile.write(json.dumps(CatItems.to_dict(),sort_keys=True,indent=4))
        for CatSearchItem in CatSearch.items_as_dicts():
            dtItem = datetime.datetime.fromisoformat(CatSearchItem['properties']['datetime']).date()
            SavePath = os.path.realpath(DataPath+CollectionId+'/'+dtItem.strftime("%Y/%m/"))
            os.makedirs(SavePath, exist_ok=True)
            with open(SavePath+'/'+CatSearchItem['id']+'.json','w') as fConfigFile:
                fConfigFile.write(json.dumps(CatSearchItem,sort_keys=True,indent=4))


def MainProcess():
    global jSources

    dtLoopEnd   = datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    dtLoopStart = (dtLoopEnd.replace(hour=0, minute=0, second=0, day=1, month=1) - datetime.timedelta(days=1)).replace(day=1, month=1) # Get First Day of past Year

    # Just for DEV Tests
    dtLoopStart = dtLoopEnd.replace(hour=0, minute=0, second=0) - datetime.timedelta(days=7)

    for Source in jSources:
        SourceData = jSources[Source]
        if (SourceData['SysName'] == 'PlanetaryComputer'):
            PlanetaryComputer(Source, dtLoopStart, dtLoopEnd)


def main():
    try:
        MainProcess()
        sys.exit(0)
    except KeyboardInterrupt:
        print("Py Geo Images Interrupted!")
        sys.exit(1)


if __name__ == "__main__":
    main()
