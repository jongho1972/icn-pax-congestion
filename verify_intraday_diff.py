"""인트라데이 검증 스냅샷 비교 도구.

backfill_excel.py가 매 호출 시 Daily_Data/_verification/<targetYMD>/<fetchYMDHHMM>.pkl
로 추가 저장한 스냅샷들을 비교해 airport.kr이 17:00 외 시각에도 데이터를
갱신하는지 확인한다.

가설(H0): airport.kr는 매일 17:00 KST에만 갱신, 그 이후엔 동일 데이터 반환
판정: 같은 targetYMD의 모든 스냅샷이 identical → H0 채택 → 23:30 백필은 백업 외 가치 없음
       차이 발견 → H0 기각 → 23:30 백필이 실제 더 신선한 데이터 캡처

실행: python3 verify_intraday_diff.py
"""
from __future__ import annotations

import pickle
import sys
from collections import defaultdict
from pathlib import Path

VERIFY_DIR = Path(__file__).resolve().parent / "Daily_Data" / "_verification"


def deep_diff(a, b, path=""):
    """leaf 단위 (path, a, b) 차이 yield."""
    if type(a) is not type(b):
        yield (path, a, b)
        return
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            yield from deep_diff(a.get(k), b.get(k), f"{path}.{k}")
    elif isinstance(a, list):
        if len(a) != len(b):
            yield (path, f"len={len(a)}", f"len={len(b)}")
            return
        for i, (x, y) in enumerate(zip(a, b)):
            yield from deep_diff(x, y, f"{path}[{i}]")
    else:
        if a != b:
            yield (path, a, b)


def load(p: Path) -> dict:
    with open(p, "rb") as f:
        d = pickle.load(f)
    # fetched_at는 캡처 시각 자체라 항상 다름 — 비교에서 제외
    d.pop("fetched_at", None)
    return d


def main() -> int:
    if not VERIFY_DIR.exists():
        print(f"검증 디렉토리 없음: {VERIFY_DIR}")
        print("백필이 1회 이상 실행돼야 스냅샷이 생성됩니다.")
        return 1

    target_dirs = sorted(p for p in VERIFY_DIR.iterdir() if p.is_dir())
    if not target_dirs:
        print("스냅샷 없음")
        return 1

    summary = []
    for td in target_dirs:
        snaps = sorted(td.glob("*.pkl"))
        if len(snaps) < 2:
            print(f"\n=== target {td.name} === ({len(snaps)} snapshots, 비교 불가)")
            continue

        print(f"\n=== target {td.name} === ({len(snaps)} snapshots)")
        base_path = snaps[0]
        base = load(base_path)
        print(f"  기준: {base_path.name}")
        for p in snaps[1:]:
            cur = load(p)
            diffs = list(deep_diff(base, cur))
            if not diffs:
                print(f"  {p.name}: identical ✓")
                summary.append((td.name, base_path.name, p.name, 0))
            else:
                print(f"  {p.name}: {len(diffs)}개 필드 다름")
                for path, a, b in diffs[:8]:
                    a_repr = repr(a)[:60]
                    b_repr = repr(b)[:60]
                    print(f"    {path}: {a_repr} → {b_repr}")
                if len(diffs) > 8:
                    print(f"    ... +{len(diffs) - 8}개 더")
                summary.append((td.name, base_path.name, p.name, len(diffs)))

    print("\n=== 요약 ===")
    if not summary:
        print("비교 가능한 페어 없음")
        return 0
    total_pairs = len(summary)
    identical = sum(1 for _, _, _, d in summary if d == 0)
    print(f"총 {total_pairs}개 비교 페어 중 {identical}개 identical, {total_pairs - identical}개 차이 발견")
    if identical == total_pairs:
        print("→ 가설(H0) 잠정 채택: airport.kr 17:00 단발 갱신 가능성 높음")
        print("→ 23:30 KST 백필은 17:05 백업 외 가치 없음 (시각 변경/제거 검토 가능)")
    else:
        print("→ 가설(H0) 기각: airport.kr이 17:00 외에도 갱신함")
        print("→ 23:30 KST 백필이 더 신선한 데이터 캡처에 실질 기여")

    return 0


if __name__ == "__main__":
    sys.exit(main())
