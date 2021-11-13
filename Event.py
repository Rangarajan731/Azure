import logging
import json
import os
from datetime import datetime,timezone
from azure.eventgrid import EventGridPublisherClient, EventGridEvent
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ServiceRequestError

# set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s : %(name)s : [%(levelname)s] : %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def send_event(endpoint: str, msg: dict) -> None:
    if isinstance(msg, str):
        logger.debug("message is in {}, converting it to <class 'dict'>".format(msg.__class__))
        try:
            msg = json.loads(msg)
        except json.decoder.JSONDecodeError as e:
            logger.exception(e)
            logger.debug('exiting')
            raise
    
    if not isinstance(msg,dict):
        logger.exception("expected <class 'str'> or <class 'dict'>, received {}".format(msg.__class__))
        logger.debug("Exiting")
        raise TypeError("expected <class 'str'> or <class 'dict'>, received {}".format(msg.__class__))
   
    
    logger.debug('event generation initiated, Fetching access key')

    # get access key, for the given endpoint
    # sample endpoint: https://<event-topic-name>.eastus-1.eventgrid.azure.net/api/events
    secret_name = endpoint[8:endpoint.index('.')]

    #keyvault name as env name
    vault_uri: str
    try:
        vault_uri = os.environ['key-vault']
        secret_client = SecretClient(vault_uri,DefaultAzureCredential())
    except KeyError as e:
        logger.error('environment variable not set.')
        logger.exception(e)
        logger.debug('exiting')
        raise
    except ServiceRequestError:
        logger.error('unable to create secret service client')
        logger.debug('exiting')
        raise
    
     #use default credential
    
    # get endpoint access key
    access_key: str
    try:
        access_key = secret_client.get_secret(secret_name).value
    except Exception as e:
        logger.exception("secret not found")
        logger.debug("exiting")
        raise
    # sys.stdout.write(access_key)
    logger.debug('Access key obtained successfully')

    #generate event
    logger.debug('generating event')
    payload = EventGridEvent(
    data=msg,
    subject= "event",
    event_type = "Azure.Sdk.Demo",
    event_time = datetime.now(timezone.utc).isoformat(),
    data_version = "1.0"
    )
    key = AzureKeyCredential(access_key)
    client = EventGridPublisherClient(endpoint,key)
    client.send(payload)
    client.close()
    logger.debug('successful')




