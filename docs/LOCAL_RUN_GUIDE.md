# Local Installation and Run

Project: Scalable Orders Data Pipeline

MongoDB database:

```text
orders_bigdata_pipeline
```

Main command:

```powershell
cd "$HOME\Desktop\Scalable_Orders_Data_Pipeline"
python -m src.pipeline_entry --input "<CSV_PATH>"
```

Route test:

```powershell
python -m src.pipeline_entry --input "<CSV_PATH>" --dry-route
```

Automated tests:

```powershell
python -m pytest tests -q
```

Upsert proof:

```powershell
python -m src.upsert_validation
```

Compliance check:

```powershell
python -m src.compliance_check
```
