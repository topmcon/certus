from certus.ingest.coinmarketcal import latest_events
j = latest_events(max_items=1)
print("CoinMarketCal ok;", "status=", "events" if isinstance(j, dict) else type(j))
