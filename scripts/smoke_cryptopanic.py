from certus.ingest.cryptopanic import latest_posts
j = latest_posts(public=True, page=1)
print("CryptoPanic ok;", "count=", len(j.get("results", [])))
