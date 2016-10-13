--
-- Minimalistic Python Redis Adapter, using UDFs for Pushdown
--
CREATE SCHEMA adapter;

CREATE OR REPLACE PYTHON ADAPTER SCRIPT adapter.redis_adapter AS

import json
import string

def adapter_call(request):
    root = json.loads(request)
    if root["type"] == "createVirtualSchema":
        return handleCreateVSchema(root)
    elif root["type"] == "dropVirtualSchema":
        return json.dumps({"type": "dropVirtualSchema"}).encode('utf-8')
    elif root["type"] == "refresh":
        return json.dumps({"type": "refresh"}).encode('utf-8')
    elif root["type"] == "setProperties":
        return json.dumps({"type": "setProperties"}).encode('utf-8')
    if root["type"] == "getCapabilities":
        return json.dumps({
            "type": "getCapabilities",
            "capabilities": ["FILTER_EXPRESSIONS","LITERAL_STRING","FN_PRED_EQUAL"]
            }).encode('utf-8') # database expects utf-8 encoded string of type str. unicode not yet supported.
    elif root["type"] == "pushdown":
        return handlePushdown(root)
    else:
        raise ValueError('Unsupported callback')

def handleCreateVSchema(root):
    res = {
        "type": "createVirtualSchema",
        "schemaMetadata": {
            "tables": [
            {
                "name": "KEY_VALS",
                "columns": [{
                    "name": "KEY",
                    "dataType": {"type": "VARCHAR", "size": 2000000}
                },{
                    "name": "VALUE",
                    "dataType": {"type": "VARCHAR", "size": 2000000}
                }]
            }]
        }
    }
    return json.dumps(res).encode('utf-8')

def handlePushdown(root):
    properties = root["schemaMetadataInfo"]["properties"]
    host = properties["REDIS_HOST"]
    port = int(properties["REDIS_PORT"])
    if "filter" in root["pushdownRequest"]:
        key = root["pushdownRequest"]["filter"]["right"]["value"]
        sql = "select adapter.redis_get('%s', %s, '%s')" % (host, port, key)
    else:
        if "DISABLE_SCAN" in properties:
            if properties["DISABLE_SCAN"].lower() == "true":
                raise RuntimeError('Full scan on redis would be required, but this was deactivated via the DISABLE_SCAN property')
        sql = "select adapter.redis_scan('%s', %s)" % (host, port)
    res = {
        "type": "pushdown",
        "sql": sql
        }
    return json.dumps(res).encode('utf-8')
/

-- The Adapter uses these UDFs for the pushdown
CREATE OR REPLACE PYTHON SET SCRIPT adapter.redis_get(host varchar(1000), port int, key varchar(1000)) EMITS (key varchar(2000000), val varchar(2000000)) AS
import redis
def run(ctx):
    r = redis.StrictRedis(host=ctx.host, port=ctx.port, db=0)
    ctx.emit(ctx.key, r.get(ctx.key))
/

CREATE OR REPLACE PYTHON SET SCRIPT adapter.redis_scan(host varchar(1000), port int) EMITS (key varchar(2000000), val varchar(2000000)) AS
import redis
def run(ctx):
    r = redis.StrictRedis(host=ctx.host, port=ctx.port, db=0)
    # Do a full iteration. Ugly, but works:-)
    offset = 0
    while True:
        res = r.scan(offset)
        offset = long(res[0])
        for key in res[1]:
            ctx.emit(key, str(r.get(key)))
        if offset == 0:
            break
/

-- The following UDF is not required, but can be used directly to work with redis
-- Would be nice to support INSERT INTO for virtual tables :-)
CREATE OR REPLACE PYTHON SET SCRIPT adapter.redis_set(host varchar(1000), port int, key varchar(1000), val varchar(1000)) EMITS (key varchar(2000000), val varchar(2000000)) AS
import redis
def run(ctx):
    r = redis.StrictRedis(host=ctx.host, port=ctx.port, db=0)
    r.set(ctx.key, ctx.val)
    ctx.emit(ctx.key, r.get(ctx.key))
/
