from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import math, random

app = FastAPI(title="Certus CP4.5 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

@app.get("/api/categories/indices")
def categories_indices(tf: str = "1D"):
    now = datetime.utcnow()
    def series(label: str, seed: float, n: int = 96):
        pts = []
        for i in range(n):
            t = now - timedelta(minutes=15*(n-i))
            v = 100 + seed + 6.0 * math.sin(i/7) + (0.5 - (i % 3)/3.0)
            pts.append({"t": int(t.timestamp()*1000), "v": v})
        return {"label": label, "color": None, "points": pts}
    return [
        series("Payment and Value", 0),
        series("Layer 1", 3),
        series("Utility", 2),
        series("DEX", -1),
        series("Gaming", 4),
        series("Culture", -2),
        series("Meme Coins", 1),
    ]

@app.get("/api/categories/heatmap")
def categories_heatmap(window: str = "24h"):
    cats = [
        ("Payment and Value", 189_000_000, +1.20),
        ("Layer 1",           98_900_000, +0.73),
        ("Utility",            8_810_000, +1.57),
        ("DEX",                6_260_000, +0.80),
        ("Gaming",             5_950_000, +7.02),
        ("Culture",            2_980_000, -1.40),
        ("Meme Coins",         1_420_000, +0.73),
        ("Infrastructure",     7_580_000, -0.28),
        ("Interoperability",   2_080_000, -0.22),
        ("Privacy",           14_010_000, +3.35),
        ("DeFi",               7_430_000, -0.39),
        ("Industry",           1_960_000, +10.61),
        ("Layer 2",            2_490_000, +0.64),
        ("NFT",                  960_000, +0.12),
    ]
    children = []
    for name, vol, chg in cats:
        children.append({
            "name": name,
            "size": vol,
            "fill": "#2ECC71" if chg >= 0 else "#E74C3C",
            "label": f"{name}\n{vol/1_000_000:.1f}M USD\n{chg:.2f}%",
        })
    return {"name": "root", "children": children}

@app.get("/api/markets/top")
def markets_top(limit: int = 25):
    def spark(base: float = 0.0, n: int = 50):
        return [base + (random.random() - 0.5) * 2 for _ in range(n)]
    rows = [
        {"market":"BTC/USD","base":"Bitcoin","quote":"US Dollar","category":"Payment and Value",
         "price":111_622,"high":111_925.9,"low":110_528.9,"change":0.93,"volume":80.3,"spark":spark(),"starred":True},
        {"market":"USDT/USD","base":"Tether USD","quote":"US Dollar","category":"Stablecoin",
         "price":1.00017,"high":1.00026,"low":0.99999,"change":0.00,"volume":65.7,"spark":spark(-0.1),"starred":False},
        {"market":"XRP/USD","base":"XRP","quote":"US Dollar","category":"Payment and Value",
         "price":2.61282,"high":2.65290,"low":2.49808,"change":4.18,"volume":39.3,"spark":spark(1.2),"starred":False},
    ]
    return rows[:limit]
