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
import pika
import requests
import dotenv
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ThisPath    = os.path.dirname(__file__)+'/'
ConfigPath  = ThisPath+'config/'
MetaPath    = ThisPath+'meta/'
LogPath     = ThisPath+'log/'
FieldDelim  = ','

if not os.path.exists(MetaPath): os.makedirs(MetaPath)
if not os.path.exists(LogPath): os.makedirs(LogPath)

# MgOBJ_CONN = None
PgSQL_CONN = None
PgSQL_CURS = None
Msg_Rabiit = None
MsgChannelPublish = None
RabiitMQ = None

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
    global PgSQL_CONN, PgSQL_CURS, Msg_Rabiit, MsgChannelPublish, RabiitMQ, ConfigPath, jSources, gStatesInterestBBOX, gCitiesInterestBBOX

    ### Env Variables
    dotenv.load_dotenv()
    PostgreSQL = {
        'HOST': os.getenv('PgSQL_HOST', ''),
        'PORT': int(os.getenv('PgSQL_PORT', 0)),
        'USER': os.getenv('PgSQL_USER', ''),
        'PASS': os.getenv('PgSQL_PASS', ''),
        'NAME': os.getenv('PgSQL_NAME', ''),
        'DEBUB': os.getenv('PgSQL_DEBUB', 'False') == "True"
    }
    RabiitMQ = {
        'HOST': os.getenv('Msg_Rabiit_HOST', 'localhost'),
        'PORT': int(os.getenv('Msg_Rabiit_PORT', '5672')),
        'QUEUE': os.getenv('Msg_Rabiit_QUEUE', 'rabbimq_queue')
    }

    ### Postgre Database
    PgSQL_CONN = psycopg2.connect (
        host=PostgreSQL['HOST'],
        port=PostgreSQL['PORT'],
        user=PostgreSQL['USER'],
        password=PostgreSQL['PASS'],
        dbname=PostgreSQL['NAME']
    )
    PgSQL_CURS = PgSQL_CONN.cursor()

    ### RabbitMQ
    Msg_Rabiit = pika.BlockingConnection(pika.ConnectionParameters(
        host=RabiitMQ['HOST'],
        port=RabiitMQ['PORT']
    ))
    MsgChannelPublish = Msg_Rabiit.channel()
    MsgChannelPublish.queue_declare(queue=RabiitMQ['QUEUE'], durable=True)

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


def CreateRetrySession(retries=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def GetPlanetaryComputer(v_Source=None, v_dtLoopStart=None, v_dtLoopEnd=None, v_bUpdateCatallog=False):
    global ExecutionId, ExecutionDt, MetaPath, LogPath, jSources, gCitiesInterestBBOX, FieldDelim

    SourceData = jSources[v_Source]
    MetaFileName = os.path.realpath(MetaPath+SourceData['SysName']+'_'+'Collections.meta.json')
    dtRangeStr = v_dtLoopStart.astimezone().isoformat()+'/'+v_dtLoopEnd.astimezone().isoformat()

    planetarycomputer_catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace)
    planetarycomputer_catalog._stac_io.session = CreateRetrySession()

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
            try:
                CatSearch = planetarycomputer_catalog.search(collections=[CollectionId], bbox=gInterestBBOX['bbox'], datetime=dtRangeStr)
                for CatSearchItem in CatSearch.items_as_dicts():
                    #CatSearchItem['_id'] = CatSearchItem['id']
                    CatSearchItem['_id'] = str(hashlib.md5((ExecutionDt+CatSearchItem['id']).encode('UTF-8')).hexdigest())
                    CatSearchItem['_dt_update'] = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
                    CatSearchItem['_ts_update'] = int(datetime.datetime.now(datetime.UTC).timestamp())
                    CatSearchItem['_query'] = {
                        'collection':CollectionId,
                        'InterestBBOX_id':gInterestBBOX['id'],
                        'InterestBBOX_name':gInterestBBOX['name'],
                        'datetime':dtRangeStr
                    }
                    CatSearchItem['_log_unique_id'] = str(hashlib.md5((CollectionId+str(gInterestBBOX['id'])+gInterestBBOX['name']+dtRangeStr+CatSearchItem['id']).encode('UTF-8')).hexdigest())

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
                        'LogUniqueId':CatSearchItem['_log_unique_id'],
                        'ExecutionId':ExecutionId,
                        'ExecutionDt':ExecutionDt,
                        'CollectionId':CollectionId,
                        'InterestBBOXId':gInterestBBOX['id'],
                        'InterestBBOXName':gInterestBBOX['name'],
                        'SearchRangeStartDt':v_dtLoopStart.astimezone().isoformat(),
                        'SearchRangeEndDt':v_dtLoopEnd.astimezone().isoformat(),
                        'MetaFileUniqueId':CatSearchItem['_id'],
                        'MetaFileDt':dtItem.isoformat(),
                        'MetaFileName':CatSearchItem['_filename']
                    }
                    LogDataArr.append(jLogData)

                    ### Save File Locally
                    if (not bFileExists):
                        os.makedirs(SavePath, exist_ok=True)
                        with open(FileName,'w') as fConfigFile:
                            fConfigFile.write(json.dumps(CatSearchItem,sort_keys=True,indent=4))
            except requests.exceptions.RequestException as e:
                print(f"[ERRO] Falha ao buscar {CollectionId} para {gInterestBBOX['name']}: {e}")
                continue

    ### Save Log File (as CSV)
    LogFileName = LogPath+SourceData['SysName']+'_'+str(CollectionId)+'_'+str(ExecutionId)+'.csv'
    with open(os.path.realpath(LogFileName),'w') as fCsvLogFile:
        fCsvLogFile.write(DictArrayToCsv(LogDataArr, FieldDelim))


def ProcessPlanetaryComputer(v_Source=None):
    global ExecutionId, LogPath, MetaPath, jSources, FieldDelim, PgSQL_CONN, PgSQL_CURS, MsgChannelPublish, RabiitMQ

    SourceData = jSources[v_Source]
    MetaFileName = os.path.realpath(MetaPath+SourceData['SysName']+'_'+'Collections.meta.json')

    ### Get Updated and Enabled Collections
    jCollections = []
    with open(MetaFileName, 'r') as fConfigFile:
        for Collections in json.load(fConfigFile):
            if (Collections['Enabled'] == True):
                jCollections.append(Collections)

    for collection in jCollections:
        CollectionId = collection['CollectionId']

        LogUniqFiles = []
        LogUniqItems = []
        LogFileName = LogPath+SourceData['SysName']+'_'+str(CollectionId)+'_'+str(ExecutionId)+'.csv'
        if os.path.isfile(LogFileName):
            with open(LogFileName, 'r') as fCsvLogFile:
                LogFile = fCsvLogFile.read()

            CsvHEader = LogFile.split('\n')[0].split(FieldDelim)
            for CsvLine in LogFile.split('\n')[1:]:
                if (len(CsvLine) == 0):
                    continue
                CsvItems = CsvLine.split(FieldDelim)
                LogUniqItems.append(CsvItems[0])
                LogUniqFiles.append(CsvItems[10])
                PgSQL_Insert_Log = ("""
                    INSERT INTO sat_images.metafiles_log (
                        log_unique_id,
                        execution_id,
                        execution_dt,
                        collection_id,
                        interest_bbox_id,
                        interest_bbox_name,
                        search_range_start_dt,
                        search_range_end_dt,
                        meta_file_id,
                        meta_file_dt,
                        meta_file_name
                    ) VALUES (
                        '{log_unique_id}',
                        '{execution_id}',
                        to_timestamp('{execution_dt}','dd-mm-yyyy hh24:mi:ss'),
                        '{collection_id}',
                        {interest_bbox_id},
                        '{interest_bbox_name}',
                        to_timestamp('{search_range_start_dt}','dd-mm-yyyy hh24:mi:ss'),
                        to_timestamp('{search_range_end_dt}','dd-mm-yyyy hh24:mi:ss'),
                        '{meta_file_id}',
                        to_timestamp('{meta_file_dt}','dd-mm-yyyy hh24:mi:ss'),
                        '{meta_file_name}'
                    );
                    """.format(
                        log_unique_id = CsvItems[0],
                        execution_id = CsvItems[1],
                        execution_dt = datetime.datetime.fromisoformat(CsvItems[2]).strftime("%d-%m-%Y %H:%M:%S"),
                        collection_id = CsvItems[3],
                        interest_bbox_id = CsvItems[4],
                        interest_bbox_name = CsvItems[5].replace("'","''"),
                        search_range_start_dt = datetime.datetime.fromisoformat(CsvItems[6]).strftime("%d-%m-%Y %H:%M:%S"),
                        search_range_end_dt = datetime.datetime.fromisoformat(CsvItems[7]).strftime("%d-%m-%Y %H:%M:%S"),
                        meta_file_id = CsvItems[8],
                        meta_file_dt = datetime.datetime.fromisoformat(CsvItems[9]).strftime("%d-%m-%Y %H:%M:%S"),
                        meta_file_name = CsvItems[10]
                    ))

                try:
                    PgSQL_CURS.execute(PgSQL_Insert_Log)
                except psycopg2.Error as e:
                    pass #print(f"[DB ERROR] {e}")

            LogUniqFiles = set(LogUniqFiles)
            LogUniqItems = set(LogUniqItems)
            PgSQL_CONN.commit()

        ### Verify If Log File is in DB
        PgSQL_Select_Log = ("""
            SELECT COUNT(*) FROM sat_images.metafiles_log WHERE
            execution_id='{execution_id}' AND collection_id='{collection_id}';
            """.format(
                execution_id = ExecutionId,
                collection_id = CollectionId
            ))
        PgSQL_CURS.execute(PgSQL_Select_Log)
        PgSQL_ROWS = PgSQL_CURS.fetchall()
        PgSQL_Result = int(PgSQL_ROWS[0][0])

        if ((PgSQL_Result == len(LogUniqItems)) and os.path.isfile(LogFileName)):
            os.remove(LogFileName)

        ### Verify If Log File is in DB
        PgSQL_Select_Files_From_Log = ("""
            SELECT DISTINCT meta_file_name FROM sat_images.metafiles_log WHERE
            execution_id='{execution_id}' AND collection_id='{collection_id}';
            """.format(
                execution_id = ExecutionId,
                collection_id = CollectionId
            ))
        PgSQL_CURS.execute(PgSQL_Select_Files_From_Log)
        PgSQL_ROWS = PgSQL_CURS.fetchall()
        PgSQL_Result = [DbItem[0] for DbItem in PgSQL_ROWS]
        PgSQL_Result.sort()

        ArrFilesToDownload = []
        for MetaFile in PgSQL_Result:
            with open(os.path.realpath(MetaFile), 'r') as fJsonMetaFIle:
                jMetaFile = json.load(fJsonMetaFIle)
                for jAssets in jMetaFile['assets']:
                    FileAssets = jMetaFile['assets'][jAssets]
                    if ('image' in FileAssets['type']):
                        ArrFilesToDownload.append({
                            'ExecutionId':ExecutionId,
                            'MetaFile':MetaFile,
                            'AssetName':jAssets,
                            'AssetTitle':FileAssets['title'],
                            'AssetType':FileAssets['type'],
                            'HrefLink':FileAssets['href']
                        })

        ### Verify Existent Files Before RabbitMQ

        for jDonFile in ArrFilesToDownload:
            MsgChannelPublish.basic_publish (
                exchange='',
                routing_key=RabiitMQ['QUEUE'],
                body=json.dumps(jDonFile),
                properties=pika.BasicProperties(delivery_mode=2)
            )


def MainProcess():
    global ExecutionId, ExecutionDt, jSources, PgSQL_CURS

    ExecutionDt = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
    ExecutionId = str(hashlib.md5((ExecutionDt).encode('UTF-8')).hexdigest())

    dtLoopEnd   = datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    dtLoopStart = (dtLoopEnd.replace(hour=0, minute=0, second=0, day=1, month=1) - datetime.timedelta(days=1)).replace(day=1, month=1) # Get First Day of past Year
    dtWeekDay   = dtLoopEnd.date().isoweekday()

    bUpdateCatallog = False
    if (dtWeekDay == 1):
        bUpdateCatallog = True

    # Just for DEV Tests
    dtLoopStart = dtLoopEnd.replace(hour=0, minute=0, second=0) - datetime.timedelta(days=7)
    ExecutionId = 'f86278350dd7c87e15b83a6627eb4f32'
    #dtLoopStart = datetime.datetime.now().replace(hour=0,  minute=0,  second=0,  microsecond=0, day=24, month=8, year=2025)
    #dtLoopEnd   = datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=0, day=24, month=8, year=2025)

    EnvironmentSetup()

    for Source in jSources:
        if (jSources[Source]['SysName'] == 'PlanetaryComputer'):
            # GetPlanetaryComputer(Source, dtLoopStart, dtLoopEnd, bUpdateCatallog)
            ProcessPlanetaryComputer(Source)


def main():
    global PgSQL_CONN, PgSQL_CURS, Msg_Rabiit

    try:
        MainProcess()
    except KeyboardInterrupt:
        print("Py Geo Images Interrupted!")
    finally:
        if Msg_Rabiit:
            Msg_Rabiit.close()
        if PgSQL_CURS:
            PgSQL_CURS.close()
        if PgSQL_CONN:
            PgSQL_CONN.close()
        sys.exit(0)


if __name__ == "__main__":
    main()

'''
DROP TABLE sat_images.metafiles_log;
CREATE TABLE sat_images.metafiles_log (
    log_unique_id           CHAR(32) PRIMARY KEY,
    execution_id            CHAR(32),
    execution_dt            TIMESTAMPTZ,
    collection_id           VARCHAR(50),
    interest_bbox_id        INTEGER,
    interest_bbox_name      VARCHAR(100),
    search_range_start_dt   TIMESTAMPTZ,
    search_range_end_dt     TIMESTAMPTZ,
    meta_file_id            CHAR(32),
    meta_file_dt            TIMESTAMPTZ,
    meta_file_name          VARCHAR(255)
);
'''
