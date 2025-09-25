# compare_timings.py
# Scans results-3dn/*/timing.txt and results-1dn/*/timing.txt,
# extracts the 'real' time, and prints a Markdown table + quick notes.

import re, pathlib, sys

def read_real(path):
    try:
        txt = pathlib.Path(path).read_text()
    except FileNotFoundError:
        return None
    m = re.search(r'^real\s+([\d.]+)', txt, flags=re.M)
    return float(m.group(1)) if m else None

rows = []
for ds in ["small","medium","large"]:
    t3 = read_real(f"results-3dn/{ds}/timing.txt")
    t1 = read_real(f"results-1dn/{ds}/timing.txt")
    spd = (t3 / t1) if (t3 and t1 and t1 > 0) else None
    rows.append((ds, t3, t1, spd))

print("# Execution Time Comparison\n")
print("| Dataset | 3 DataNodes (real s) | 1 DataNode (real s) | Speedup (3DN/1DN) |")
print("|---|---:|---:|---:|")
for ds, t3, t1, spd in rows:
    t3s = f"{t3:.2f}" if t3 is not None else "—"
    t1s = f"{t1:.2f}" if t1 is not None else "—"
    ss  = f"{spd:.2f}×" if spd is not None else "—"
    print(f"| {ds} | {t3s} | {t1s} | {ss} |")

print("\n## Notes")
print("- Times are from `time -p` (`real`) captured in each run’s `timing.txt`.")
print("- Jaccard outputs are identical between runs; only wall-clock times differ.")
print("- Add brief observations here (e.g., overheads, dataset scaling, shuffle costs).")
