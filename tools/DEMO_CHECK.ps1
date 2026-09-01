$ErrorActionPreference = "Stop"
Set-Location "%USERPROFILE%\Desktop\Scalable_Orders_Data_Pipeline"
python -m pytest tests -q
python -m src.pipeline_entry --input "%USERPROFILE%\Desktop\VIVA_TEST_DATA\orders_demo_small_10k.csv" --dry-route
python -m src.pipeline_entry --input "%USERPROFILE%\Desktop\VIVA_TEST_DATA\orders_demo_large_650k.csv" --dry-route
Write-Host "DEMO CHECK: PASS"
