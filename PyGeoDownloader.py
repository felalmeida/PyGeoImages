#!/usr/bin/python3
# -*- coding: UTF-8 -*-
###############################################################################
# Module:   PyGeoDownloader.py      Autor: Felipe Almeida                     #
# Start:    02-Mar-2026             LastUpdate: 05-Mar-2026     Version: 1.0  #
###############################################################################

import sys
import os
import argparse
import logging
import dotenv
import datetime
import hashlib
import base64
import requests
import urllib.parse
import azure.storage.blob
import json
import uuid
import pika

Logger      = logging.getLogger(__name__)
ThisPath    = os.path.dirname(__file__)+'/'

TokenDict   = None
GlobalVars  = None
AzureBlob   = None
RabbitMQ    = None
ExecutionId = ''
ExecutionDt = ''
CollectionId = ''


def stat_to_dict(filestat):
    if isinstance(filestat, os.stat_result):
        return {attr: getattr(filestat, attr) for attr in dir(filestat) if attr.startswith('st_')}
    else:
        raise ValueError(f'Argument must be os.stat_result, not {type(filestat)}')


def DownloadFile(v_DownloadFileUrl=None, v_OutputFile=None):
    global TokenDict

    if TokenDict['expiry'] < datetime.datetime.now(datetime.UTC).astimezone():
        GetPlanetaryComputerToken(CollectionId)

    try:
        DictReturn = {}
        DictReturn['Md5Sum'] = {}
        DictReturn['Headers'] = {}
        DictReturn['FileStat'] = {}

        DownloadUrlBase = v_DownloadFileUrl.split('?')[0]
        DownloadUrlParams = urllib.parse.urlsplit(v_DownloadFileUrl).query

        ParamsAsDict = urllib.parse.parse_qs(DownloadUrlParams)
        ParamsAsDict = {k: v[0] for k, v in ParamsAsDict.items()}

        TokenAsDict = urllib.parse.parse_qs(TokenDict['token'])
        TokenAsDict = {k: v[0] for k, v in TokenAsDict.items()}

        for key, value in ParamsAsDict.items():
            if key in TokenAsDict:
                if value != TokenAsDict[key]:
                    ParamsAsDict[key] = TokenAsDict[key]

        NewDownloadUrlParams = urllib.parse.urlencode(ParamsAsDict)
        PcSignedUrl = DownloadUrlBase+'?'+NewDownloadUrlParams
        response = requests.get(PcSignedUrl, stream=True, timeout=120)
        response.raise_for_status()
        expected_size = int(response.headers.get('content-length', 0))

        for key, value in response.headers.items():
            DictReturn['Headers'][key] = value

        os.makedirs(os.path.dirname(v_OutputFile), exist_ok=True)

        bytes_downloaded = 0
        with open(v_OutputFile, 'wb') as fOutputFile:
            for FileChunk in response.iter_content(chunk_size=8192):
                fOutputFile.write(FileChunk)
                bytes_downloaded += len(FileChunk)

        with open(v_OutputFile, 'rb') as fOutputFile:
            file_md5 = hashlib.md5(fOutputFile.read()).digest()
        DictReturn['Md5Sum'] = file_md5.hex()
        DictReturn['Md5Sum64'] = base64.b64encode(file_md5).decode('ascii')
        DictReturn['Md5Bytes'] = bytearray(file_md5)

        if expected_size is not None and int(expected_size) > 0 and int(expected_size) != bytes_downloaded:
            raise IOError(f"Esperado {expected_size} bytes, recebido {bytes_downloaded}")

        file_stat = os.stat(v_OutputFile)
        if file_stat.st_size != bytes_downloaded:
            raise IOError(f"Arquivo em disco {file_stat.st_size} bytes, esperado {bytes_downloaded}")

        Logger.info(f"Arquivo {os.path.basename(v_OutputFile)} ({file_stat.st_size/1024/1024:.2f} MB) baixado com sucesso.")
        DictReturn['FileStat'] = stat_to_dict(file_stat)
        return DictReturn
    except Exception as e:
        Logger.error(f"Erro ao baixar o arquivo {v_DownloadFileUrl} -> {os.path.basename(v_OutputFile)}: {e}")
        raise IOError(f"Erro ao baixar o arquivo {v_DownloadFileUrl} -> {os.path.basename(v_OutputFile)}: {e}")


def OnMessageReceived(v_MsgChannel=None, v_MsgMethod=None, v_MsgProperties=None, v_MsgBody=None):
    global Logger, ThisPath, CollectionId, AzureBlob, RabbitMQ

    if v_MsgChannel is None:
        return None

    try:
        v_MsgChannel.basic_ack(delivery_tag=v_MsgMethod.delivery_tag)

        jMsgContent = json.loads(v_MsgBody.decode('UTF-8'))
        MetaFileDt  = datetime.datetime.fromisoformat(jMsgContent['MetaFileDt'])

        Logger.info(f"MetaFileDt: {jMsgContent['MetaFileDt']}, Routing Key: {v_MsgMethod.routing_key}, Message ID: {v_MsgProperties.message_id}, MetaFileUniqueId: {jMsgContent['MetaFileUniqueId']}, AssetName: {jMsgContent['AssetName']}")

        DownloadFileUrl = jMsgContent['HrefLink']
        FileExtension = DownloadFileUrl.split('?')[0].split('.')[-1]

        DownloadFileType = jMsgContent['AssetType']
        DownloadFileName = jMsgContent['AssetDownloadFileName']+'.'+FileExtension
        AzureBlobPath = (
            jMsgContent['CollectionId'] + '/' +
            MetaFileDt.strftime('%Y') + '/' +
            MetaFileDt.strftime('%Y-%m-%d') + '/' +
            jMsgContent['AssetName'].replace(' ','_').strip().upper()
        )

        FileStatus = {}
        FileStatus['Md5Bytes'] = {}

        '''
        OutputFile = os.path.join(ThisPath, 'temp_downloads', DownloadFileName)
        FileStatus = DownloadFile(v_DownloadFileUrl=DownloadFileUrl,v_OutputFile=OutputFile)
        ThisBlobClient = AzureBlob['Client'].get_blob_client(AzureBlobPath+'/'+DownloadFileName)
        ThisBlobClient.upload_blob(
            data=open(OutputFile,'rb'),
            overwrite=True,
            content_settings=azure.storage.blob.ContentSettings(content_md5=FileStatus['Md5Bytes']),
            validate_content=True
        )
        BlobProperties = ThisBlobClient.get_blob_properties()
        if BlobProperties.size != os.path.getsize(OutputFile):
            raise IOError(f"Integridade falhou: tamanho local={os.path.getsize(OutputFile)}, blob={BlobProperties.size}")
        Logger.info(f"Upload verificado: {DownloadFileName} ({BlobProperties.size/1024/1024:.2f} MB, MD5 Ok)")
        if os.path.exists(OutputFile):
            try:
                os.remove(OutputFile)
                if not os.path.exists(OutputFile):
                    Logger.info(f"Arquivo '{OutputFile}' foi apagado localmente.")
            except FileNotFoundError:
                Logger.info(f"Error: File '{OutputFile}' not found.")
            except PermissionError:
                Logger.info(f"Error: Permission denied to delete the file '{OutputFile}'.")
            except OSError as e:
                Logger.info(f"Error occurred: {e}")
        '''

        del FileStatus['Md5Bytes']
        v_MsgChannel.basic_publish(
            exchange='',
            routing_key=RabbitMQ['ProcessPrefix']+RabbitMQ['QUEUE']+'_'+CollectionId,
            body=json.dumps({
                'OriginalMsgId':v_MsgProperties.message_id,
                'ProcessedDt':datetime.datetime.now(datetime.UTC).astimezone().isoformat(),
                'MetaFileUniqueId':jMsgContent['MetaFileUniqueId'],
                'MetaFileId':jMsgContent['MetaFileId'],
                'MetaFileDt':jMsgContent['MetaFileDt'],
                'AssetName':jMsgContent['AssetName'],
                'AssetDownloadFileName':jMsgContent['AssetDownloadFileName'],
                'AzureBlobPath':AzureBlob['Name']+'/'+AzureBlobPath+'/'+DownloadFileName,
                'FileStatus':FileStatus
            }),
            properties=pika.BasicProperties(
                message_id=str(uuid.uuid4()),
                content_type='application/json',
                delivery_mode=2
            )
        )
    except Exception as e:
        Logger.error(f"Erro ao processar mensagem: {e}")
        v_MsgChannel.basic_nack(delivery_tag=v_MsgMethod.delivery_tag, requeue=True)


def GetPlanetaryComputerToken(v_CollectionId=None):
    global TokenDict

    if v_CollectionId is None:
        return None

    ApiKeyUrl = 'https://planetarycomputer.microsoft.com/api/sas/v1/token/'+str(v_CollectionId)
    KeyResponse = requests.get(ApiKeyUrl, timeout=30)
    KeyResponse.raise_for_status()
    KeyDict = KeyResponse.json()
    KeyDict['expiry'] = datetime.datetime.fromisoformat(KeyDict['msft:expiry'])
    TokenDict = KeyDict.copy()


def EnvironmentSetup(v_EnvFile='.env', v_LogLevel=None):
    global GlobalVars, AzureBlob, RabbitMQ, CollectionId

    ### Env Variables
    dotenv.load_dotenv(os.path.realpath(v_EnvFile))
    GlobalVars = {
        'LogLevel': os.getenv('LogLevel', 'NOTSET').upper().strip(),
        'TimeZone': os.getenv('TimeZone', 'America/Sao_Paulo')
    }
    if v_LogLevel is not None:
        GlobalVars['LogLevel'] = v_LogLevel.upper().strip()

    AzureBlob = {
        'ConnString': os.getenv('AZURE_STORAGE_CONNECTION_STRING', None),
        'Name': os.getenv('AZURE_STORAGE_CONTAINER_NAME', None)
    }
    AzureBlob['Service'] = azure.storage.blob.BlobServiceClient.from_connection_string(AzureBlob['ConnString'])
    AzureBlob['Client'] = AzureBlob['Service'].get_container_client(AzureBlob['Name'])

    RabbitMQ = {
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
    logging.getLogger('azure.storage.blob').setLevel(logging.WARNING)
    logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

    if (RabbitMQ['ENABLE']):
        RabbitMQ['CONN'] = pika.BlockingConnection(pika.ConnectionParameters(
            host=RabbitMQ['HOST'],
            port=RabbitMQ['PORT'],
            heartbeat=600,
            blocked_connection_timeout=300
        ))
        RabbitMQ['CHANNEL'] = RabbitMQ['CONN'].channel()
        RabbitMQ['CHANNEL'].confirm_delivery()
        RabbitMQ['CHANNEL'].queue_declare(queue=RabbitMQ['QUEUE']+'_'+CollectionId, durable=True)
        RabbitMQ['CHANNEL'].queue_declare(queue=RabbitMQ['ProcessPrefix']+RabbitMQ['QUEUE']+'_'+CollectionId, durable=True)


def ParseArgs():
    parser = argparse.ArgumentParser(description='PyGeo Images Downloader')

    parser.add_argument('-s', type=str, default=None, action='store', dest='Source',
                        help='Fonte de Dados (Staelite)', required=True)

    parser.add_argument('--env-file', type=str, default='.env',
                        help='Caminho para o arquivo .env (default: .env)')

    parser.add_argument('--log-level', type=str, default='info',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Nível de log (sobrescreve o .env)')

    return parser.parse_args()


def MainProcess():
    global Logger, ExecutionId, ExecutionDt, CollectionId, GlobalVars, RabbitMQ

    args = ParseArgs()

    ExecutionDt = datetime.datetime.now(datetime.UTC).astimezone().isoformat()
    ExecutionId = str(hashlib.md5((ExecutionDt).encode('UTF-8')).hexdigest())

    CollectionId = args.Source.strip().lower()
    if CollectionId is None or CollectionId == '' or CollectionId == 'empty':
        Logger.error('Source não pode ser vazio.')
        return None

    EnvironmentSetup(
        v_EnvFile=args.env_file.strip().lower(),
        v_LogLevel=args.log_level.strip().upper()
    )

    GetPlanetaryComputerToken(CollectionId)

    if RabbitMQ['ENABLE'] and RabbitMQ['CHANNEL']:
        RabbitMQ['CHANNEL'].basic_qos(prefetch_count=1)
        for MsgMethod, MsgProperties, MsgBody in RabbitMQ['CHANNEL'].consume(
                queue=RabbitMQ['QUEUE']+'_'+CollectionId,
                auto_ack=RabbitMQ['AutoAck'],
                inactivity_timeout=RabbitMQ['TimeOut']):
            if MsgMethod is None:
                Logger.info('Nenhuma mensagem recebida em '+str(RabbitMQ['TimeOut'])+'s. Encerrando.')
                return None
            OnMessageReceived(RabbitMQ['CHANNEL'], MsgMethod, MsgProperties, MsgBody)


def main():
    global RabbitMQ

    try:
        MainProcess()
    except KeyboardInterrupt:
        print("Py Geo Images Downloader Interrupted!")
    except Exception as e:
        print(f"[ERRO] {e}")
    finally:
        if (RabbitMQ):
            if (RabbitMQ['CHANNEL']):
                RabbitMQ['CHANNEL'].close()
            if (RabbitMQ['CONN']):
                RabbitMQ['CONN'].close()


if __name__ == "__main__":
    main()

