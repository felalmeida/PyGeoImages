#!/usr/bin/python3
# -*- coding: UTF-8 -*-
###############################################################################
# Module:   PyGeoImages.py          Autor: Felipe Almeida                     #
# Start:    05-Feb-2026             LastUpdate: 05-Mar-2026     Version: 1.0  #
###############################################################################
"""Módulo de coleta e processamento de metadados de imagens geoespaciais de satélite.

Este módulo realiza a busca, coleta e armazenamento de metadados de imagens
de satélite a partir de catálogos STAC (SpatioTemporal Asset Catalog), como o
Microsoft Planetary Computer. Os metadados são filtrados por áreas de interesse
(estados e municípios brasileiros) e podem ser persistidos em PostgreSQL,
MongoDB (DocumentDB) ou arquivos locais (CSV/JSON). Também suporta o envio de
tarefas de download via RabbitMQ.

Dependências externas:
    - pystac_client: Cliente para catálogos STAC.
    - planetary_computer: Autenticação com o Microsoft Planetary Computer.
    - geojson / turfpy: Manipulação de dados geoespaciais e cálculo de bounding boxes.
    - psycopg2: Conexão com PostgreSQL.
    - pymongo: Conexão com MongoDB / DocumentDB.
    - pika: Conexão com RabbitMQ.
    - dotenv: Carregamento de variáveis de ambiente a partir de arquivo .env.

Exemplo de uso:
    Executar diretamente via linha de comando::

        $ python PyGeoImages.py
"""

import sys
import os
import logging
import datetime
import json
import uuid
import pystac_client
import planetary_computer
import geojson
import turfpy.measurement
import hashlib
import pika
import pymongo
import requests
import dotenv
import psycopg2
import psycopg2.extras
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

Logger      = logging.getLogger(__name__)
ThisPath    = os.path.dirname(__file__)+'/'
ConfigPath  = ThisPath+'config/'
MetaPath    = ThisPath+'meta/'
LogPath     = ThisPath+'log/'
FieldDelim  = ','

if not os.path.exists(MetaPath): os.makedirs(MetaPath)
if not os.path.exists(LogPath): os.makedirs(LogPath)

GlobalVars = None
PgSQL = None
MgObj = None
RabiitMQ = None

ExecutionId = ''
ExecutionDt = ''
jSources = {}
gStatesInterestBBOX = []
gCitiesInterestBBOX = []


def DictArrayToCsv(v_jArray, v_FieldDelim=','):
    """Converte uma lista de dicionários em uma string no formato CSV.

    Campos cujo nome inicia com underscore ('_') são ignorados na conversão.

    Args:
        v_jArray (list[dict]): Lista de dicionários a ser convertida. Todos os
            dicionários devem possuir as mesmas chaves. O primeiro elemento é
            utilizado para extrair o cabeçalho.
        v_FieldDelim (str, optional): Caractere delimitador de campos.
            Padrão: ``','``.

    Returns:
        str: String contendo o conteúdo CSV com cabeçalho e linhas de dados.
    """
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


def CsvToDictArray(v_CsvStr, v_FieldDelim=','):
    """Converte uma string CSV em uma lista de dicionários.

    A primeira linha da string é tratada como cabeçalho, definindo as chaves
    dos dicionários resultantes. Linhas vazias são ignoradas.

    Args:
        v_CsvStr (str): String contendo o conteúdo CSV completo (cabeçalho + dados).
        v_FieldDelim (str, optional): Caractere delimitador de campos.
            Padrão: ``','``.

    Returns:
        list[dict]: Lista de dicionários onde cada dicionário representa uma
        linha do CSV, com as chaves definidas pelo cabeçalho.
    """
    CsvLines = v_CsvStr.split('\n')
    CsvHeader = CsvLines[0].split(v_FieldDelim)
    DictArray = []
    for CsvLine in CsvLines[1:]:
        if (len(CsvLine) == 0):
            continue
        CsvItems = CsvLine.split(v_FieldDelim)
        jItem = {}
        for i, Field in enumerate(CsvHeader):
            jItem[Field] = CsvItems[i]
        DictArray.append(jItem)

    return DictArray


def jLogDataToSQL(v_jLogData):
    """Gera uma instrução SQL INSERT para inserir um registro de log na tabela ``sat_images.metafiles_log``.

    Os campos de data/hora são convertidos do formato ISO 8601 para o formato
    ``dd-mm-yyyy hh24:mi:ss`` compatível com a função ``to_timestamp`` do PostgreSQL.

    Args:
        v_jLogData (dict): Dicionário contendo os dados do registro de log com
            as seguintes chaves obrigatórias:

            - ``LogUniqueId`` (str): Identificador único do registro de log (MD5).
            - ``ExecutionId`` (str): Identificador da execução (MD5).
            - ``ExecutionDt`` (str): Data/hora da execução em formato ISO 8601.
            - ``CollectionId`` (str): Identificador da coleção STAC.
            - ``InterestBBOXId`` (int): Identificador da área de interesse (BBOX).
            - ``InterestBBOXName`` (str): Nome da área de interesse.
            - ``SearchRangeStartDt`` (str): Início do intervalo de busca (ISO 8601).
            - ``SearchRangeEndDt`` (str): Fim do intervalo de busca (ISO 8601).
            - ``MetaFileUniqueId`` (str): Identificador único do metadado (MD5).
            - ``MetaFileDt`` (str): Data/hora do metadado (ISO 8601).
            - ``MetaFileId`` (str): Identificador original do item no catálogo.
            - ``MetaFileAssets`` (str): Lista de assets do metadado.
            - ``MetaFileName`` (str): Nome/caminho do arquivo de metadado.

    Returns:
        str: String contendo a instrução SQL INSERT formatada.
    """
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
            meta_file_unique_id,
            meta_file_dt,
            meta_file_mgrs,
            meta_file_id,
            meta_file_assets,
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
            '{meta_file_unique_id}',
            to_timestamp('{meta_file_dt}','dd-mm-yyyy hh24:mi:ss'),
            {meta_file_mgrs},
            '{meta_file_id}',
            '{meta_file_assets}',
            '{meta_file_name}'
        );
        """.format(
            log_unique_id = v_jLogData['LogUniqueId'],
            execution_id = v_jLogData['ExecutionId'],
            execution_dt = datetime.datetime.fromisoformat(v_jLogData['ExecutionDt']).strftime("%d-%m-%Y %H:%M:%S"),
            collection_id = v_jLogData['CollectionId'],
            interest_bbox_id = v_jLogData['InterestBBOXId'],
            interest_bbox_name = v_jLogData['InterestBBOXName'].replace("'","''"),
            search_range_start_dt = datetime.datetime.fromisoformat(v_jLogData['SearchRangeStartDt']).strftime("%d-%m-%Y %H:%M:%S"),
            search_range_end_dt = datetime.datetime.fromisoformat(v_jLogData['SearchRangeEndDt']).strftime("%d-%m-%Y %H:%M:%S"),
            meta_file_unique_id = v_jLogData['MetaFileUniqueId'],
            meta_file_dt = datetime.datetime.fromisoformat(v_jLogData['MetaFileDt']).strftime("%d-%m-%Y %H:%M:%S"),
            meta_file_mgrs = "'"+v_jLogData['MetaFileMgrs']+"'" if v_jLogData['MetaFileMgrs'] is not None else 'NULL',
            meta_file_id = v_jLogData['MetaFileId'],
            meta_file_assets = v_jLogData['MetaFileAssets'].replace("'","''"),
            meta_file_name = v_jLogData['MetaFileName']
        )
    )
    return PgSQL_Insert_Log


def EnvironmentSetup():
    """Inicializa e configura todo o ambiente de execução do módulo.

    Esta função realiza as seguintes operações:

    1. Carrega variáveis de ambiente do arquivo ``.env`` via ``dotenv``.
    2. Configura o nível de logging conforme a variável ``LogLevel``.
    3. Estabelece conexão com o PostgreSQL (se habilitado via ``PgSQL_ENABLE``).
    4. Estabelece conexão com o MongoDB/DocumentDB (se habilitado via ``MgOBJ_ENABLE``).
    5. Carrega as configurações de fontes de dados (``Sources.json``), filtrando
       apenas as fontes habilitadas.
    6. Carrega e filtra os estados brasileiros habilitados (``Estados.json``) e
       seus respectivos dados geográficos (``Estados_GeoJS.json``).
    7. Carrega e filtra os municípios brasileiros habilitados (``Municipios.json``)
       e seus respectivos dados geográficos (``Municipios_GeoJS.json``).
    8. Calcula as bounding boxes (BBOX) das áreas de interesse para estados e
       municípios utilizando ``turfpy``.

    Modifica as variáveis globais:
        GlobalVars, PgSQL, MgObj, RabiitMQ, jSources,
        gStatesInterestBBOX, gCitiesInterestBBOX.

    Raises:
        psycopg2.OperationalError: Se a conexão com o PostgreSQL falhar.
        pymongo.errors.ConnectionFailure: Se a conexão com o MongoDB falhar.
        FileNotFoundError: Se algum arquivo de configuração não for encontrado.
    """
    global GlobalVars, PgSQL, MgObj, RabiitMQ, ConfigPath, jSources, gStatesInterestBBOX, gCitiesInterestBBOX

    ### Env Variables
    dotenv.load_dotenv()
    GlobalVars = {
        'LogLevel': os.getenv('LogLevel', 'NOTSET').upper().strip(),
        'TimeZone': os.getenv('TimeZone', 'America/Sao_Paulo')
    }
    PgSQL = {
        'HOST': os.getenv('PgSQL_HOST', 'localhost'),
        'PORT': int(os.getenv('PgSQL_PORT', '5432')),
        'USER': os.getenv('PgSQL_USER', 'user'),
        'PASS': os.getenv('PgSQL_PASS', 'pass'),
        'NAME': os.getenv('PgSQL_NAME', 'dbname'),
        'ENABLE': os.getenv('PgSQL_ENABLE', 'False') == 'True',
        'CONN':None,
        'CURS':None
    }
    MgObj = {
        'HOST': os.getenv('MgOBJ_HOST', 'localhost'),
        'PORT': int(os.getenv('MgOBJ_PORT', '27017')),
        'USER': os.getenv('MgOBJ_USER', 'user'),
        'PASS': os.getenv('MgOBJ_PASS', 'pass'),
        'NAME': os.getenv('MgOBJ_NAME', 'dbname'),
        'PARAMS': os.getenv('MgOBJ_PARAMS', ''),
        'ENABLE': os.getenv('MgOBJ_ENABLE', 'False') == 'True',
        'CONN':None,
        'DB':None
    }
    RabiitMQ = {
        'HOST': os.getenv('Msg_Rabiit_HOST', 'localhost'),
        'PORT': int(os.getenv('Msg_Rabiit_PORT', '5672')),
        'QUEUE': os.getenv('Msg_Rabiit_QUEUE', 'rabbimq_queue'),
        'RoutingKey': os.getenv('Msg_Rabiit_RoutingKey', 'routing_key'),
        'ProcessPrefix': os.getenv('Msg_Rabiit_ProcessPrefix', 'PROCESS_'),
        'AutoAck': os.getenv('Msg_Rabiit_AutoAck', 'False') == 'True',
        'TimeOut': int(os.getenv('Msg_Rabiit_TimeOut', '15')),
        'ENABLE': os.getenv('Msg_Rabiit_ENABLE', 'False') == 'True',
        'CONN':None,
        'CHANNEL':None
    }

    LogFormat = ('%(levelname) -5s %(asctime)s %(name) -15s %(funcName) -25s %(lineno) -5d: %(message)s')

    if GlobalVars['LogLevel'] == 'DEBUG':
        logging.basicConfig(level=logging.DEBUG, format=LogFormat)
    elif GlobalVars['LogLevel'] == 'INFO':
        logging.basicConfig(level=logging.INFO, format=LogFormat)
    elif GlobalVars['LogLevel'] == 'WARNING':
        logging.basicConfig(level=logging.WARNING, format=LogFormat)
    elif GlobalVars['LogLevel'] == 'ERROR':
        logging.basicConfig(level=logging.ERROR, format=LogFormat)
    elif GlobalVars['LogLevel'] == 'CRITICAL':
        logging.basicConfig(level=logging.CRITICAL, format=LogFormat)
    else:
        logging.basicConfig(level=logging.NOTSET, format=LogFormat)

    logging.getLogger('pika').setLevel(logging.WARNING)
    #logging.getLogger('pymongo').setLevel(logging.WARNING)
    logging.getLogger('psycopg2').setLevel(logging.WARNING)

    ### Postgre Database
    if (PgSQL['ENABLE']):
        PgSQL['CONN'] = psycopg2.connect (
            host=PgSQL['HOST'],
            port=PgSQL['PORT'],
            user=PgSQL['USER'],
            password=PgSQL['PASS'],
            dbname=PgSQL['NAME']
        )
        PgSQL['CURS'] = PgSQL['CONN'].cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        PgSQL['CURS'].execute("SET TIME ZONE '"+GlobalVars['TimeZone']+"';")

    ### DocumentDB
    if (MgObj['ENABLE']):
        MgObj['CONN'] = pymongo.MongoClient(
            host=MgObj['HOST'],
            port=MgObj['PORT']
        )
        # MgObj['CONN'] = pymongo.MongoClient('mongodb+srv://'+MgObj['USER']+':'+MgObj['PASS']+'@'+MgObj['HOST']+'/?'+MgObj['PARAMS'])
        MgObj['DB'] = MgObj['CONN'][MgObj['NAME']]

    ### RabbitMQ
    # if (RabiitMQ['ENABLE']):
    #     RabiitMQ['CONN'] = pika.BlockingConnection(pika.ConnectionParameters(
    #         host=RabiitMQ['HOST'],
    #         port=RabiitMQ['PORT'],
    #         heartbeat=600,
    #         blocked_connection_timeout=300
    #     ))
    #     RabiitMQ['CHANNEL'] = RabiitMQ['CONN'].channel()
    #     RabiitMQ['CHANNEL'].queue_declare(queue=RabiitMQ['QUEUE'], durable=True)

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


def RabbitMQEnsureConnected(v_CollectionId=None, v_ForceReconnect=False):
    """Garante que a conexão com o RabbitMQ esteja ativa e funcional.

    Verifica o estado da conexão e do canal RabbitMQ. Caso a conexão ou o canal
    estejam fechados ou inválidos, realiza a reconexão automática, incluindo a
    redeclaração da fila configurada.

    A reconexão utiliza os parâmetros definidos na variável global ``RabiitMQ``
    (HOST, PORT, QUEUE), com ``heartbeat=600`` e ``blocked_connection_timeout=300``.

    Modifica a variável global:
        RabiitMQ (dict): Atualiza as chaves ``'CONN'`` e ``'CHANNEL'`` em caso
        de reconexão.
    """
    global RabiitMQ

    if RabiitMQ['ENABLE']:
        needs_reconnect = False
        try:
            if v_ForceReconnect:
                needs_reconnect = True
            elif RabiitMQ['CONN'] is None or not RabiitMQ['CONN'].is_open:
                needs_reconnect = True
            elif RabiitMQ['CHANNEL'] is None or not RabiitMQ['CHANNEL'].is_open:
                needs_reconnect = True
        except Exception:
            needs_reconnect = True

        if needs_reconnect:
            try:
                if RabiitMQ['CONN'] and RabiitMQ['CONN'].is_open:
                    RabiitMQ['CONN'].close()
            except Exception:
                pass

            MsgCollId = '_'+str(v_CollectionId).strip().replace(' ','-') if v_CollectionId is not None else '_GENERAL'
            RabiitMQ['CONN'] = pika.BlockingConnection(pika.ConnectionParameters(
                host=RabiitMQ['HOST'],
                port=RabiitMQ['PORT'],
                heartbeat=600,
                blocked_connection_timeout=300
            ))
            RabiitMQ['CHANNEL'] = RabiitMQ['CONN'].channel()
            RabiitMQ['CHANNEL'].queue_declare(queue=RabiitMQ['QUEUE']+MsgCollId, durable=True)


def GetMetadataData(v_CollectionId=None, v_MetaFileUniqueId=None, v_MetaFileName=None):
    """Recupera os dados de metadado de um item do catálogo STAC.

    Busca o documento de metadado no MongoDB/DocumentDB (quando habilitado) ou
    lê o arquivo JSON local correspondente.

    Args:
        v_CollectionId (str, optional): Identificador da coleção STAC. Utilizado
            como nome da collection no MongoDB.
        v_MetaFileUniqueId (str, optional): Identificador único (MD5) do
            documento de metadado no MongoDB.
        v_MetaFileName (str, optional): Caminho completo do arquivo JSON de
            metadado no sistema de arquivos local.

    Returns:
        dict or None: Dicionário contendo os dados do metadado, ou ``None``
        caso o documento não seja encontrado ou ocorra um erro no MongoDB.
    """
    global MgObj

    MetaData = None
    if (not MgObj['ENABLE']):
        with open(os.path.realpath(v_MetaFileName), 'r') as fJsonMetaFIle:
            MetaData = json.load(fJsonMetaFIle)
    else:
        try:
            MetaData = MgObj['DB'][v_CollectionId].find_one({'_id': v_MetaFileUniqueId})
        except Exception as e:
            print(f"[MgOBJ ERROR] {e}")

    return MetaData


def GetLogRecords(v_Source=None, v_CollectionId=None, v_ForceCollection=False):
    """Recupera os registros de log de uma execução para uma coleção específica.

    Busca registros de log no PostgreSQL (quando habilitado) ou lê o arquivo CSV
    local correspondente. Os registros são filtrados pelo ``ExecutionId`` atual
    e pelo ``CollectionId`` informado.

    Args:
        v_Source (str, optional): Chave da fonte de dados no dicionário
            ``jSources``. Utilizado para determinar o nome do arquivo CSV local.
        v_CollectionId (str, optional): Identificador da coleção STAC para
            filtrar os registros de log.

    Returns:
        list[dict]: Lista de dicionários contendo os registros de log. Cada
        dicionário possui as chaves: ``LogUniqueId``, ``ExecutionId``,
        ``ExecutionDt``, ``CollectionId``, ``InterestBBOXId``,
        ``InterestBBOXName``, ``SearchRangeStartDt``, ``SearchRangeEndDt``,
        ``MetaFileUniqueId``, ``MetaFileDt``, ``MetaFileId``, ``MetaFileAssets``,
        ``MetaFileName``.
    """
    global Logger, ExecutionId, LogPath, PgSQL, MgObj, jSources, FieldDelim

    ReturnArr = []
    if (not PgSQL['ENABLE']):
        SourceData = jSources[v_Source]
        LogFileName = LogPath+SourceData['SysName']+'_'+str(v_CollectionId)+'_'+str(ExecutionId)+'.csv'
        if os.path.isfile(LogFileName):
            with open(LogFileName, 'r') as fCsvLogFile:
                LogFile = fCsvLogFile.read()
            ReturnArr = CsvToDictArray(LogFile, FieldDelim)
    else:
        SqlWhere = ("""WHERE execution_id = '{execution_id}' AND collection_id='{collection_id}'""".format(execution_id = ExecutionId, collection_id = v_CollectionId))
        if (v_ForceCollection):
            SqlWhere = ("""WHERE collection_id='{collection_id}'""".format(collection_id = v_CollectionId))
        PgSQL_Select_From_Log = ("""
            SELECT log_unique_id,execution_id,execution_dt,collection_id,interest_bbox_id,
            interest_bbox_name,search_range_start_dt,search_range_end_dt,meta_file_unique_id,
            meta_file_dt,meta_file_mgrs,meta_file_id,meta_file_assets,meta_file_name
            FROM sat_images.vw_metafiles_log_most_recent """+SqlWhere+"""
            ORDER BY meta_file_dt DESC;""")
        PgSQL['CURS'].execute(PgSQL_Select_From_Log)
        while True:
            PgSQL_ROWS = PgSQL['CURS'].fetchmany(10000)
            if not PgSQL_ROWS:
                break
            for DbItem in PgSQL_ROWS:
                LogItem = {
                    'LogUniqueId':        DbItem['log_unique_id'],
                    'ExecutionId':        DbItem['execution_id'],
                    'ExecutionDt':        DbItem['execution_dt'].isoformat(),
                    'CollectionId':       DbItem['collection_id'],
                    'InterestBBOXId':     DbItem['interest_bbox_id'],
                    'InterestBBOXName':   DbItem['interest_bbox_name'],
                    'SearchRangeStartDt': DbItem['search_range_start_dt'].isoformat(),
                    'SearchRangeEndDt':   DbItem['search_range_end_dt'].isoformat(),
                    'MetaFileUniqueId':   DbItem['meta_file_unique_id'],
                    'MetaFileDt':         DbItem['meta_file_dt'].isoformat(),
                    'MetaFileMgrs':       DbItem['meta_file_mgrs'],
                    'MetaFileId':         DbItem['meta_file_id'],
                    'MetaFileAssets':     DbItem['meta_file_assets'],
                    'MetaFileName':       DbItem['meta_file_name']
                }
                ReturnArr.append(LogItem)
        Logger.info(f"Total de registros de log lidos: {len(ReturnArr)}")

    return ReturnArr


def CreateRetrySession(retries=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504)):
    """Cria uma sessão HTTP com política de retentativas automáticas.

    Configura uma sessão ``requests.Session`` com um ``HTTPAdapter`` que aplica
    retentativas automáticas para os métodos HEAD, GET e POST em caso de falhas
    de conexão, leitura ou códigos de status HTTP específicos.

    Args:
        retries (int, optional): Número máximo de retentativas para cada tipo
            de falha (total, leitura, conexão). Padrão: ``3``.
        backoff_factor (int, optional): Fator multiplicador para o tempo de espera
            entre retentativas (backoff exponencial). Padrão: ``1``.
        status_forcelist (tuple[int], optional): Tupla de códigos de status HTTP
            que devem acionar retentativa. Padrão: ``(429, 500, 502, 503, 504)``.

    Returns:
        requests.Session: Sessão HTTP configurada com retentativas para
        protocolos HTTPS e HTTP.
    """
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
    """Coleta metadados de imagens de satélite do Microsoft Planetary Computer.

    Conecta-se ao catálogo STAC do Planetary Computer e realiza buscas de itens
    para cada coleção habilitada, cruzando com as áreas de interesse (bounding
    boxes dos municípios). Os metadados encontrados são armazenados no MongoDB
    ou em arquivos JSON locais, e os registros de log são salvos no PostgreSQL
    ou em arquivos CSV locais.

    Opcionalmente, atualiza o catálogo local de coleções disponíveis no
    Planetary Computer.

    Args:
        v_Source (str, optional): Chave da fonte de dados no dicionário
            ``jSources`` (ex.: nome configurado em ``Sources.json``).
        v_dtLoopStart (datetime.datetime, optional): Data/hora de início do
            intervalo de busca no catálogo STAC.
        v_dtLoopEnd (datetime.datetime, optional): Data/hora de fim do
            intervalo de busca no catálogo STAC.
        v_bUpdateCatallog (bool, optional): Se ``True``, atualiza o arquivo
            de metadados das coleções disponíveis no Planetary Computer antes
            de iniciar a coleta. Padrão: ``False``.

    Raises:
        requests.exceptions.RequestException: Capturada internamente para cada
            combinação de coleção/BBOX; a busca continua com os próximos itens.
    """
    global ExecutionId, ExecutionDt, MetaPath, LogPath, jSources, gCitiesInterestBBOX, FieldDelim, PgSQL, MgObj, RabiitMQ

    SourceData = jSources[v_Source]
    CollectionsMetaFileName = os.path.realpath(MetaPath+SourceData['SysName']+'_'+'Collections.meta.json')

    planetarycomputer_catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace)
    planetarycomputer_catalog._stac_io.session = CreateRetrySession()

    if (v_bUpdateCatallog):
        ### Organize Collections in Meta File Keeping Enable Status
        ArrCollections = []
        jCollections = []

        if os.path.exists(CollectionsMetaFileName):
            with open(CollectionsMetaFileName, 'r') as fConfigFile:
                jCollections = json.load(fConfigFile)

        for collection in list(planetarycomputer_catalog.get_collections()):
            DctCollection = collection.to_dict()
            ItEnabled = True
            ItTimeDelta = 7
            LocalData = None
            for Collection in jCollections:
                if (Collection['CollectionId'] == DctCollection['id']):
                    LocalData = Collection
                    break
            if (LocalData is not None):
                if ('Enabled' in LocalData):
                    ItEnabled = LocalData['Enabled']
                if ('TimeDelta' in LocalData):
                    ItTimeDelta = LocalData['TimeDelta']

            jCollection = {
                'Enabled':ItEnabled,
                'TimeDelta':ItTimeDelta,
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
        with open(CollectionsMetaFileName,'w') as fConfigFile:
            fConfigFile.write(json.dumps(ArrCollections,sort_keys=True,indent=4))
        del ArrCollections
        del jCollections

    ### Get Updated and Enabled Collections
    jCollections = []
    with open(CollectionsMetaFileName, 'r') as fConfigFile:
        for Collections in json.load(fConfigFile):
            if (Collections['Enabled'] == True):
                jCollections.append(Collections)

    ### Get Metadata for Selected Dates, Collections and Interests BBOX
    LogDataArr = []
    for collection in jCollections:
        CollectionId = collection['CollectionId']
        dtLoopStart = v_dtLoopStart # - datetime.timedelta(days=collection['TimeDelta'])
        dtRangeStr = dtLoopStart.astimezone().isoformat()+'/'+v_dtLoopEnd.astimezone().isoformat()

        Logger.info(f"Buscando itens para o satélite {CollectionId} no intervalo ({collection['TimeDelta']} dias) {dtRangeStr} cruzando com municípios")
        CollectionItensCount = 0
        for gInterestBBOX in gCitiesInterestBBOX:
            try:
                CatSearch = planetarycomputer_catalog.search(collections=[CollectionId], bbox=gInterestBBOX['bbox'], datetime=dtRangeStr)
                for CatSearchItem in CatSearch.items_as_dicts():
                    CollectionItensCount += 1
                    CatSearchItem['_id'] = str(hashlib.md5((CollectionId+CatSearchItem['id']).encode('UTF-8')).hexdigest())
                    CatSearchItem['_dt_update'] = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
                    CatSearchItem['_ts_update'] = int(datetime.datetime.now(datetime.UTC).timestamp())
                    CatSearchItem['_query'] = {
                        'collection':CollectionId,
                        'InterestBBOX_id':gInterestBBOX['id'],
                        'InterestBBOX_name':gInterestBBOX['name'],
                        'datetime':dtRangeStr
                    }
                    CatSearchItem['_log_unique_id'] = str(hashlib.md5((str(CollectionItensCount)+CatSearchItem['id']+ExecutionId+CollectionId+dtRangeStr+str(gInterestBBOX['id'])).encode('UTF-8')).hexdigest())
                    dtItemStr = None

                    if (CatSearchItem['properties']['datetime'] is not None):
                        dtItemStr = CatSearchItem['properties']['datetime']
                    elif (CatSearchItem['properties']['start_datetime'] is not None):
                        dtItemStr = CatSearchItem['properties']['start_datetime']
                    elif (CatSearchItem['properties']['end_datetime'] is not None):
                        dtItemStr = CatSearchItem['properties']['end_datetime']

                    CatSearchItem['_dt_meta_file'] = dtItemStr
                    dtItem = datetime.datetime.fromisoformat(dtItemStr)
                    SavePath = os.path.realpath(MetaPath+CollectionId+'/'+dtItem.strftime("%Y%m%d")+'/'+str(gInterestBBOX['id']))
                    FileName = SavePath+'/'+CatSearchItem['id']+'.json'
                    ActualFileName = FileName

                    ### Search For Duplicated Files
                    LogStrMgOBJ = ''
                    if (MgObj['ENABLE']):
                        bDocumentExists = False
                        try:
                            if MgObj['DB'][CollectionId].count_documents({'_id': CatSearchItem['_id']}):
                                bDocumentExists = True
                                LogStrMgOBJ = '(Metadados existente para '+CatSearchItem['_id']+')'
                                ActualFileName = MgObj['DB'][CollectionId].find_one({'_id': CatSearchItem['_id']})['_filename']
                        except Exception as e:
                            print(f"[MgOBJ ERROR] {e}")
                    else:
                        bFileExists = False
                        for root, dirs, files in os.walk(os.path.realpath(MetaPath+CollectionId+'/'+dtItem.strftime("%Y%m%d")+'/')):
                            if CatSearchItem['id']+'.json' in files:
                                bFileExists = True
                                ActualFileName = os.path.join(root, CatSearchItem['id']+'.json')

                    CatSearchItem['_filename'] = ActualFileName
                    ### Save To DocumentDB
                    if (MgObj['ENABLE']):
                        if (not bDocumentExists):
                            try:
                                MgObj['DB'][CollectionId].insert_one(CatSearchItem)
                            except pymongo.errors.DuplicateKeyError:
                                print(f"[DB WARNING] Documento duplicado para {CatSearchItem['_id']} na coleção {CollectionId}")
                    else:
                        ### Save File Locally
                        if (not bFileExists):
                            os.makedirs(SavePath, exist_ok=True)
                            with open(FileName,'w') as fConfigFile:
                                fConfigFile.write(json.dumps(CatSearchItem,sort_keys=True,indent=4))

                    ### MGRS Tile - Military Grid Reference System
                    MetaFileMgrs = None
                    if ('s2:mgrs_tile' in CatSearchItem['properties']):
                        MetaFileMgrs = CatSearchItem['properties']['s2:mgrs_tile']

                    ### Save Reference Log to Array
                    jLogData = {
                        'LogUniqueId':CatSearchItem['_log_unique_id'],
                        'ExecutionId':ExecutionId,
                        'ExecutionDt':ExecutionDt,
                        'CollectionId':CollectionId,
                        'InterestBBOXId':gInterestBBOX['id'],
                        'InterestBBOXName':gInterestBBOX['name'],
                        'SearchRangeStartDt':dtLoopStart.astimezone().isoformat(),
                        'SearchRangeEndDt':v_dtLoopEnd.astimezone().isoformat(),
                        'MetaFileMgrs':MetaFileMgrs,
                        'MetaFileUniqueId':CatSearchItem['_id'],
                        'MetaFileDt':dtItem.astimezone().isoformat(),
                        'MetaFileId':CatSearchItem['id'],
                        'MetaFileAssets':'['+'|'.join(list(CatSearchItem['assets'].keys()))+']',
                        'MetaFileName':CatSearchItem['_filename']
                    }
                    Logger.debug(jLogData['MetaFileDt']+' - '+jLogData['CollectionId']+' - '+jLogData['InterestBBOXName']+' - '+CatSearchItem['id']+' | '+LogStrMgOBJ)
                    LogDataArr.append(jLogData)
            except requests.exceptions.RequestException as e:
                print(f"[ERRO] Falha ao buscar {CollectionId} para {gInterestBBOX['name']}: {e}")
                continue
        Logger.info(f"Quantidade de Itens Obtidos para o satélite {CollectionId}: {CollectionItensCount}")

    ### Save Log To Database
    if (PgSQL['ENABLE']):
        nItemCount = 0
        for jLogData in LogDataArr:
            nItemCount += 1
            try:
                PgSQL['CURS'].execute(jLogDataToSQL(jLogData))
            except psycopg2.Error as e:
                print(f"[PgSQL ERROR] {e}")
                print(jLogDataToSQL(jLogData))
                sys.exit(1)

        if (nItemCount % 1000 == 0):
            print(f"[PgSQL] Comitando registros a cada 1000 itens. Total até agora: {nItemCount}")
            PgSQL['CONN'].commit()
        PgSQL['CONN'].commit()
    else:
        ### Save Log File (as CSV)
        LogFileName = LogPath+SourceData['SysName']+'_'+str(CollectionId)+'_'+str(ExecutionId)+'.csv'
        with open(os.path.realpath(LogFileName),'w') as fCsvLogFile:
            fCsvLogFile.write(DictArrayToCsv(LogDataArr, FieldDelim))

    del LogDataArr


def ProcessPlanetaryComputer(v_Source=None):
    """Processa metadados coletados e enfileira tarefas de download via RabbitMQ.

    Para cada coleção habilitada, recupera os registros de log da execução atual,
    carrega os metadados correspondentes e identifica os assets de imagem
    disponíveis para download. As informações de cada asset são publicadas como
    mensagens na fila RabbitMQ para processamento posterior (download).

    Args:
        v_Source (str, optional): Chave da fonte de dados no dicionário
            ``jSources`` (ex.: nome configurado em ``Sources.json``).

    Raises:
        pika.exceptions.AMQPConnectionError: Se a conexão com o RabbitMQ for
            perdida durante a publicação. O processamento é interrompido.
        pika.exceptions.AMQPChannelError: Se o canal RabbitMQ se tornar
            inválido. O processamento é interrompido.
    """
    global Logger, ExecutionId, LogPath, MetaPath, jSources, FieldDelim, PgSQL, MgObj, RabiitMQ

    SourceData = jSources[v_Source]
    CollectionsMetaFileName = os.path.realpath(MetaPath+SourceData['SysName']+'_'+'Collections.meta.json')

    ### Get Updated and Enabled Collections
    jCollections = []
    with open(CollectionsMetaFileName, 'r') as fConfigFile:
        for Collections in json.load(fConfigFile):
            if (Collections['Enabled'] == True):
                jCollections.append(Collections)

    bForceCollection = False
    for collection in jCollections:
    #for collection in [{'CollectionId':'sentinel-1-rtc'}]:
        CollectionId = collection['CollectionId']
        Logger.info(f"Obtendo registros de log para: {CollectionId}")
        LogRecords = GetLogRecords(v_Source, CollectionId, bForceCollection)
        MetaFileUniqueIdList = list(dict.fromkeys([LogItem['MetaFileUniqueId'] for LogItem in LogRecords]))

        Logger.info(f"Processando {len(MetaFileUniqueIdList)} arquivos para download para: {CollectionId}")

        ArrFilesToDownload = []
        for MetaFileUniqueId in MetaFileUniqueIdList:
            MetaFileDt, MetaFileMgrs, MetaFileId, MetaFileName = next((Item['MetaFileDt'],Item['MetaFileMgrs'],Item['MetaFileId'],Item['MetaFileName']) for Item in LogRecords if Item['MetaFileUniqueId'] == MetaFileUniqueId)
            jMetaFile = GetMetadataData(CollectionId, MetaFileUniqueId, MetaFileName)
            nAssetCount = 0
            for AssetName in jMetaFile['assets']:
                FileAssets = jMetaFile['assets'][AssetName]
                if ('image' in FileAssets['type']):
                    nAssetCount += 1
                    ArrFilesToDownload.append({
                        'ExecutionId':ExecutionId,
                        'CollectionId':CollectionId,
                        'MetaFileUniqueId':MetaFileUniqueId,
                        'MetaFileDt':MetaFileDt,
                        'MetaFileMgrs':MetaFileMgrs,
                        'MetaFileId':MetaFileId,
                        'AssetDownloadFileName':str(MetaFileId+'_'+AssetName).replace(' ','_').strip().upper(),
                        'AssetName':AssetName,
                        'AssetTitle':FileAssets['title'],
                        'AssetType':FileAssets['type'],
                        'HrefLink':FileAssets['href']
                    })
            # Logger.info(f"Arquivo {MetaFileId} possui {nAssetCount} ativos de download.")

        ### Verify Existent Files Before RabbitMQ

        MsgCollId = '_'+str(CollectionId).strip().replace(' ','-') if CollectionId is not None else '_GENERAL'
        RabbitMQEnsureConnected(CollectionId, True)
        Logger.info(f"Enviando {len(ArrFilesToDownload)} arquivos para download via RabbitMQ ({RabiitMQ['RoutingKey']+MsgCollId})")
        for jDonFile in ArrFilesToDownload:
            try:
                Logger.debug(f"MetaFileDt: {jDonFile['MetaFileDt']},  MetaFileUniqueId: {jDonFile['MetaFileUniqueId']}, AssetName: {jDonFile['AssetName']}")
                RabiitMQ['CHANNEL'].basic_publish (
                    exchange='',
                    routing_key=RabiitMQ['RoutingKey']+MsgCollId,
                    body=json.dumps(jDonFile),
                    properties=pika.BasicProperties (
                        message_id=str(uuid.uuid4()),
                        content_type='application/json',
                        delivery_mode=2
                    )
                )
            except pika.exceptions.AMQPConnectionError as e:
                print(f"[ERRO] Conexão com RabbitMQ perdida: {e}")
                break
            except pika.exceptions.AMQPChannelError as e:
                print(f"[ERRO] Canal RabbitMQ inválido: {e}")
                break
            except Exception as e:
                print(f"[ERRO] Falha ao publicar mensagem {jDonFile}: {e}")


def MainProcess():
    """Função principal que orquestra a execução completa do pipeline de coleta.

    Realiza as seguintes etapas:

    1. Gera um identificador único de execução (``ExecutionId``) baseado em MD5
       da data/hora atual.
    2. Define o intervalo de datas para busca (do início do ano anterior até hoje).
    3. Determina se o catálogo de coleções deve ser atualizado (às segundas-feiras).
    4. Chama ``EnvironmentSetup()`` para inicializar o ambiente.
    5. Itera sobre as fontes de dados habilitadas e executa a coleta para cada
       fonte do tipo ``PlanetaryComputer``.

    Modifica as variáveis globais:
        ExecutionId, ExecutionDt.
    """
    global Logger, ExecutionId, ExecutionDt, jSources

    for dtYear in range(2024,2009,-1):

        ExecutionDt = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
        ExecutionId = str(hashlib.md5((ExecutionDt).encode('UTF-8')).hexdigest())

        dtWeekDay   = datetime.datetime.now().date().isoweekday()
        dtLoopEnd   = datetime.datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
        dtLoopStart = dtLoopEnd.replace(hour=0, minute=0, second=0, microsecond=0)

        dtLoopEnd   = dtLoopEnd.replace(day=31, month=12, year=dtYear, hour=23, minute=59, second=59, microsecond=999999)
        dtLoopStart = dtLoopEnd.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0)

        bUpdateCatallog = False
        if (dtWeekDay == 1):
            bUpdateCatallog = True

        EnvironmentSetup()

        for Source in jSources:
            if (jSources[Source]['SysName'] == 'PlanetaryComputer'):
                GetPlanetaryComputer(Source, dtLoopStart, dtLoopEnd, bUpdateCatallog)
                #ProcessPlanetaryComputer(Source)


def main():
    """Ponto de entrada do módulo. Executa o pipeline e gerencia o ciclo de vida das conexões.

    Chama ``MainProcess()`` dentro de um bloco try/except/finally para garantir
    que todas as conexões com bancos de dados (PostgreSQL, MongoDB) e filas
    (RabbitMQ) sejam encerradas corretamente, independentemente de sucesso,
    interrupção pelo usuário (``KeyboardInterrupt``) ou erro inesperado.
    """
    global PgSQL, MgObj, RabiitMQ

    try:
        MainProcess()
    except KeyboardInterrupt:
        print("Py Geo Images Interrupted!")
    except Exception as e:
        print(f"[ERRO] {e}")
    finally:
        if (PgSQL):
            if (PgSQL['CURS']):
                PgSQL['CURS'].close()
            if (PgSQL['CONN']):
                PgSQL['CONN'].close()
        if (MgObj):
            if (MgObj['ENABLE']):
                MgObj['CONN'].close()
        if (RabiitMQ):
            if (RabiitMQ['CHANNEL']):
                RabiitMQ['CHANNEL'].close()
            if (RabiitMQ['CONN']):
                RabiitMQ['CONN'].close()


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
    meta_file_unique_id     CHAR(32),
    meta_file_dt            TIMESTAMPTZ,
    meta_file_mgrs          VARCHAR(10),
    meta_file_id            VARCHAR(100),
    meta_file_assets        VARCHAR(400),
    meta_file_name          VARCHAR(255)
);
CREATE INDEX idx_execution_id ON sat_images.metafiles_log (execution_id);
CREATE INDEX idx_metafile_bbox_dt ON sat_images.metafiles_log (meta_file_id, interest_bbox_id, execution_dt DESC);
'''

'''
DROP VIEW sat_images.vw_metafiles_log_most_recent;
CREATE OR REPLACE VIEW sat_images.vw_metafiles_log_most_recent AS
SELECT DISTINCT ON (meta_file_id, interest_bbox_id)
    log_unique_id,
    execution_id,
    execution_dt,
    collection_id,
    interest_bbox_id,
    interest_bbox_name,
    search_range_start_dt,
    search_range_end_dt,
    meta_file_unique_id,
    meta_file_dt,
    meta_file_mgrs,
    meta_file_id,
    meta_file_assets,
    meta_file_name
FROM sat_images.metafiles_log
ORDER BY meta_file_id, interest_bbox_id, execution_dt DESC;
'''
