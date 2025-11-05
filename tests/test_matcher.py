import subprocess
from pathlib import Path


def test_build_symbol_map_creates_csv(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    out = repo / "mappings" / "symbol_to_cg.csv"
    # run the script for a small limit to keep test fast
    res = subprocess.run(["python3", "scripts/build_symbol_map.py", "--limit", "50"], cwd=str(repo))
    assert res.returncode == 0
    assert out.exists()
    content = out.read_text()
    assert "BTC" in content
