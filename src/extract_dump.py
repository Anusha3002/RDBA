"""Extract a ListenBrainz listens dump (.tar.zst or .tar.xz) without needing the zstd command.
Usage: python src/extract_dump.py <dump file> <out_dir>   (needs: pip install zstandard)"""
import sys, tarfile, os
f, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
if f.endswith(".zst"):
    import zstandard
    with open(f, "rb") as fh, zstandard.ZstdDecompressor().stream_reader(fh) as r:
        with tarfile.open(fileobj=r, mode="r|") as t:
            t.extractall(out, filter="data") if hasattr(tarfile, "data_filter") else t.extractall(out)
else:
    with tarfile.open(f) as t:
        t.extractall(out, filter="data") if hasattr(tarfile, "data_filter") else t.extractall(out)
print("extracted to", out)
