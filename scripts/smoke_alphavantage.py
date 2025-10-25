from certus.ingest.alphavantage_client import global_quote
j = global_quote("IBM")
print("AlphaVantage ok;", "keys=", list(j.keys())[:3])
