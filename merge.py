import sqlite3
from pathlib import Path

DB_PATH = Path(r"E:\Anything\Projects\kaerna-group\csharp\Parser\Infastructure\tracker.db")


def main():
    print("Working with DB:", DB_PATH.resolve())
    if not DB_PATH.exists():
        raise SystemExit(f"DB file not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("PRAGMA foreign_keys = ON;")

    # 1. Get all players
    cur.execute("""
        SELECT "Id", "Nickname", "Discriminator", "ProfileUrl"
        FROM "Players"
        ORDER BY "Id"
    """)
    rows = cur.fetchall()
    print(f"Total players: {len(rows)}")

    # 2. Group by (Nickname, Discriminator, ProfileUrl)
    groups = {}
    for r in rows:
        key = (r["Nickname"], r["Discriminator"], r["ProfileUrl"])
        groups.setdefault(key, []).append(r["Id"])

    # Take only groups with more than 1 player — these are duplicates
    dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"Groups with duplicates: {len(dup_groups)}")

    total_deleted = 0
    total_links_updated = 0

    for (nickname, disc, url), ids in dup_groups.items():
        # Sort ids, first (minimum) is canonical
        ids_sorted = sorted(ids)
        canonical_id = ids_sorted[0]
        duplicate_ids = ids_sorted[1:]

        print(f"\n=== {nickname} #{disc}")
        print(f"Canonical Id: {canonical_id}, duplicates: {duplicate_ids}")

        for dup_id in duplicate_ids:
            # 3. Reassign matches to canonical player
            cur.execute(
                """
                UPDATE "MatchPlayer"
                SET "PlayersId" = ?
                WHERE "PlayersId" = ?
                """,
                (canonical_id, dup_id)
            )
            updated = cur.rowcount
            total_links_updated += updated
            print(f"  MatchPlayer: PlayersId {dup_id} -> {canonical_id}, updated rows: {updated}")

            # 4. Delete duplicate from Players
            cur.execute(
                'DELETE FROM "Players" WHERE "Id" = ?',
                (dup_id,)
            )
            deleted = cur.rowcount
            total_deleted += deleted
            print(f"  Deleted player Id={dup_id}, rows deleted: {deleted}")

    conn.commit()
    conn.close()

    print("\n=== Summary ===")
    print(f"Deleted duplicate players: {total_deleted}")
    print(f"Updated links in MatchPlayer: {total_links_updated}")


if __name__ == "__main__":
    main()
