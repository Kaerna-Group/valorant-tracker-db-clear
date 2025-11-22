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

    # 1. Select players with '#' in Discriminator
    cur.execute("""
        SELECT "Id", "Nickname", "Discriminator", "ProfileUrl"
        FROM "Players"
        WHERE "Discriminator" LIKE '%#%'
    """)
    rows = cur.fetchall()
    print(f"Players with '#': {len(rows)}")

    updated_disc_only = 0
    merged_players = 0
    match_links_updated = 0
    players_deleted = 0

    for row in rows:
        player_id = row["Id"]
        nickname = row["Nickname"]
        disc = row["Discriminator"] or ""
        profile_url = row["ProfileUrl"]

        # Normalize discriminator: remove all '#' and strip whitespace
        norm_disc = disc.replace("#", "").strip()

        # 2. Find canonical player without '#'
        cur.execute(
            '''
            SELECT "Id", "Nickname", "Discriminator", "ProfileUrl"
            FROM "Players"
            WHERE "Nickname" = ?
              AND "ProfileUrl" = ?
              AND "Discriminator" = ?
              AND "Id" <> ?
            LIMIT 1
            ''',
            (nickname, profile_url, norm_disc, player_id)
        )
        canonical = cur.fetchone()

        if canonical:
            canonical_id = canonical["Id"]
            print(f"[MERGE] {nickname}: '{disc}' (Id={player_id}) -> '{canonical['Discriminator']}' (Id={canonical_id})")

            # 2.1. Reassign all matches
            cur.execute(
                '''
                UPDATE "MatchPlayer"
                SET "PlayersId" = ?
                WHERE "PlayersId" = ?
                ''',
                (canonical_id, player_id)
            )
            links = cur.rowcount
            match_links_updated += links

            # 2.2. Delete duplicate
            cur.execute(
                'DELETE FROM "Players" WHERE "Id" = ?',
                (player_id,)
            )
            players_deleted += cur.rowcount
            merged_players += 1

        else:
            # 3. No canonical found — just clean the value
            print(f"[UPDATE] {nickname}: '{disc}' -> '{norm_disc}' (Id={player_id})")
            cur.execute(
                '''
                UPDATE "Players"
                SET "Discriminator" = ?
                WHERE "Id" = ?
                ''',
                (norm_disc, player_id)
            )
            updated_disc_only += cur.rowcount

    conn.commit()
    conn.close()

    print("\n=== Summary ===")
    print(f"Total players with '#': {len(rows)}")
    print(f"Updated Discriminator only: {updated_disc_only}")
    print(f"Merged players (reassignment + deletion): {merged_players}")
    print(f"Updated links in MatchPlayer: {match_links_updated}")
    print(f"Deleted duplicates from Players: {players_deleted}")

if __name__ == "__main__":
    main()
