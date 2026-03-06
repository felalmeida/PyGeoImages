#!/usr/bin/python3
# -*- coding: UTF-8 -*-
###############################################################################
# Module:   PyGeoVerifier.py        Autor: Felipe Almeida                     #
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
import urllib.parse
import json
import uuid
import pika
import time

Logger      = logging.getLogger(__name__)
ThisPath    = os.path.dirname(__file__)+'/'

TokenDict   = None
GlobalVars  = None
RabbitMQ    = None
ExecutionId = ''
ExecutionDt = ''
CollectionId = ''


def OnMessageReceived(v_MsgChannel=None, v_MsgMethod=None, v_MsgProperties=None, v_MsgBody=None):
    global Logger, ThisPath, CollectionId, RabbitMQ

    if v_MsgChannel is None:
        return None

    try:
        #v_MsgChannel.basic_ack(delivery_tag=v_MsgMethod.delivery_tag)

        jMsgContent = json.loads(v_MsgBody.decode('UTF-8'))
        Logger.info(json.dumps(jMsgContent))

        time.sleep(1)
    except Exception as e:
        Logger.error(f"Erro ao processar mensagem: {e}")
        v_MsgChannel.basic_nack(delivery_tag=v_MsgMethod.delivery_tag, requeue=True)


def EnvironmentSetup(v_EnvFile='.env', v_LogLevel=None):
    global GlobalVars, RabbitMQ, CollectionId

    ### Env Variables
    dotenv.load_dotenv(os.path.realpath(v_EnvFile))
    GlobalVars = {
        'LogLevel': os.getenv('LogLevel', 'NOTSET').upper().strip(),
        'TimeZone': os.getenv('TimeZone', 'America/Sao_Paulo')
    }
    if v_LogLevel is not None:
        GlobalVars['LogLevel'] = v_LogLevel.upper().strip()

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

    if RabbitMQ['ENABLE'] and RabbitMQ['CHANNEL']:
        RabbitMQ['CHANNEL'].basic_qos(prefetch_count=1)
        for MsgMethod, MsgProperties, MsgBody in RabbitMQ['CHANNEL'].consume(
                queue=RabbitMQ['ProcessPrefix']+RabbitMQ['QUEUE']+'_'+CollectionId,
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

