from sensapp_sdk import SensAppClient

with SensAppClient("http://127.0.0.1:3000") as client:
    table = client.query_arrow('temperature{room="lab"}[6h]')
    print(table.schema)
    print(table.to_pylist()[:5])
