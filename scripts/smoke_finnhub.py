from certus.ingest.finnhub_client import quote
j = quote("AAPL")
print("Finnhub ok;", "fields=", list(j.keys())[:5])
