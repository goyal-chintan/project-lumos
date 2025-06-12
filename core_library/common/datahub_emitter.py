# DataHub Emitter abstraction

from datahub.emitter.rest_emitter import DatahubRestEmitter

def get_emitter(server_url):
    return DatahubRestEmitter(gms_server=server_url)