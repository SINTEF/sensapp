from sensapp_sdk import SensAppClient

with SensAppClient("http://127.0.0.1:3000") as client:
    catalog = client.list_series(metric="temperature", limit=5)
    datasets = catalog.get("dcat:dataset", [])

    for dataset in datasets:
        series_uuid = dataset.get("dct:identifier")
        title = dataset.get("dct:title")
        print(series_uuid, title)

        if series_uuid:
            rows = client.get_series_rows(series_uuid, limit=3)
            print(rows)
