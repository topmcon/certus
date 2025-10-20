certus/
  __init__.py
  config.py                    # or config/__init__.py if you prefer a folder
  data/
    __init__.py
    coingecko_client.py
    fetch_markets.py           # <-- defines fetch_markets_df, validate_markets, save_markets, main()
  storage/
    __init__.py
    io.py
scripts/
  fetch_markets.py             # <-- tiny wrapper that calls certus.data.fetch_markets.main()
pyproject.toml
