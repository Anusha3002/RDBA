"""SYNTHETIC TEST DATA ONLY. Generates fake listens in the ListenBrainz JSON-lines format so the
pipeline can be tested without the real dump. Never use its output as results."""
import json, random, sys, csv, os
random.seed(7)
out = sys.argv[1]; os.makedirs(out, exist_ok=True)
tracks = []
with open(sys.argv[2]) as f:
    for r in csv.DictReader(f):
        tracks.append((r["artists"].split(";")[0], r["track_name"]))
tracks = tracks[:2000]
n = 20000
with open(f"{out}/synthetic.listens", "w") as fh:
    for i in range(n):
        a, t = random.choice(tracks)
        if random.random() < 0.3:  # simulate tracks not in Spotify
            a, t = f"unknown artist {random.randint(1,500)}", f"song {random.randint(1,3000)}"
        rec = {"track_metadata": {"artist_name": a.title(), "track_name": t.title(), "release_name": "r",
                                  "additional_info": {"music_service": "spotify.com"}},
               "listened_at": 1730000000 + random.randint(0, 86400 * 20),
               "user_name": f"user{random.randint(1,300)}", "recording_msid": f"msid{i}"}
        if random.random() < 0.4:
            rec["track_metadata"]["mbid_mapping"] = {"recording_mbid": f"{random.getrandbits(32):08x}-aaaa-bbbb-cccc-000000000000",
                                                    "artist_mbids": [f"{random.getrandbits(32):08x}-1111-2222-3333-000000000000"]}
        fh.write(json.dumps(rec) + "\n")
        if random.random() < 0.01: fh.write(json.dumps(rec) + "\n")         # duplicate
        if random.random() < 0.005: fh.write("{bad json\n")                  # corrupt line
        if random.random() < 0.005:                                          # missing title
            rec2 = dict(rec); rec2["track_metadata"] = {"artist_name": a}; fh.write(json.dumps(rec2) + "\n")
