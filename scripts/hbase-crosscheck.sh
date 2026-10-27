#!/usr/bin/env bash
#
# Cross checks this crate against a real HBase installation, in both directions:
#
#   HBase -> us : HBase writes every encoding/codec combination, we must print the same cells
#   us -> HBase : we write files, HBase's own reader must print the same cells
#
# Requirements:
#   * HBase 2.5.x and its Hadoop dependencies on HBASE_CLASSPATH
#   * a JDK (javac/java) on PATH
#   * HBASE_SITE_DIR pointing at a directory that contains hbase-site.xml, if the cluster needs
#     one to select the zstd or snappy codec (see docs/hfile-format.md#compression)
#
# `snappy` needs a codec HBase can load: either libhadoop built with snappy, or the pure Java
# `org.apache.hadoop.hbase.io.compress.xerial.SnappyCodec` selected with
# `hbase.io.compress.snappy.codec`. Both write the same bytes, so either works here.
#
# Usage:
#   export HBASE_CLASSPATH="/opt/hbase/lib/*:/opt/hadoop/share/hadoop/common/*:..."
#   export HBASE_SITE_DIR=/etc/hbase/conf          # optional
#   CROSSCHECK_CODECS="none gz snappy zstd" scripts/hbase-crosscheck.sh
#
# Environment:
#   HFILE_BIN          binary to test              (default: target/release/hfile)
#   CROSSCHECK_CODECS  codecs to test              (default: "none gz")
#   CROSSCHECK_ROWS    rows per generated file     (default: 300)
#   ZSTD_FRAMING       raw|hbase for our zstd      (default: raw)
#   KEEP=1             keep the work directory

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
REPO=$(pwd)
HFILE_BIN=${HFILE_BIN:-$REPO/target/release/hfile}
CROSSCHECK_CODECS=${CROSSCHECK_CODECS:-"none gz"}
CROSSCHECK_ROWS=${CROSSCHECK_ROWS:-300}
ZSTD_FRAMING=${ZSTD_FRAMING:-raw}

if [ -z "${HBASE_CLASSPATH:-}" ]; then
  echo "error: set HBASE_CLASSPATH to the HBase/Hadoop jars" >&2
  exit 2
fi

CP=$HBASE_CLASSPATH
if [ -n "${HBASE_SITE_DIR:-}" ]; then
  CP=$HBASE_SITE_DIR:$CP
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/hfile-crosscheck.XXXXXX")
cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "work directory kept at $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

mkdir -p "$WORK/classes"
if ! javac -nowarn -cp "$CP" -d "$WORK/classes" "$REPO"/scripts/hbase/*.java; then
  echo "error: cannot compile the HBase helpers; check HBASE_CLASSPATH" >&2
  exit 2
fi
JAVA="java -cp $WORK/classes:$CP"

if [ ! -x "$HFILE_BIN" ]; then
  echo "building $HFILE_BIN"
  cargo build --release >/dev/null || exit 2
fi

pass=0
fail=0
ok() { pass=$((pass + 1)); echo "OK   $*"; }
ko() { fail=$((fail + 1)); echo "FAIL $*"; }

java_codec() {
  case "$1" in
    none) echo NONE ;;
    gz) echo GZ ;;
    snappy) echo SNAPPY ;;
    zstd) echo ZSTD ;;
    *) echo NONE ;;
  esac
}

echo "== HBase -> hfile-rs"
REFERENCE=""
for enc in NONE PREFIX DIFF FAST_DIFF ROW_INDEX_V1; do
  for codec in $CROSSCHECK_CODECS; do
    file=$WORK/hbase_${enc}_${codec}.hfile
    if ! $JAVA WriteHFile "$file" "$enc" true false "$CROSSCHECK_ROWS" \
        "$(java_codec "$codec")" >/dev/null 2>"$WORK/write.err"; then
      ko "hbase write $enc/$codec: $(tail -1 "$WORK/write.err")"
      continue
    fi
    [ -z "$REFERENCE" ] && REFERENCE=$file
    if ! $JAVA DumpHFile "$file" >"$WORK/ref.txt" 2>"$WORK/ref.err"; then
      ko "hbase read $enc/$codec: $(tail -1 "$WORK/ref.err")"
      continue
    fi
    if ! "$HFILE_BIN" dump "$file" >"$WORK/ours.txt" 2>"$WORK/ours.err"; then
      ko "hfile-rs read $enc/$codec: $(tail -1 "$WORK/ours.err")"
      continue
    fi
    if diff -q "$WORK/ref.txt" "$WORK/ours.txt" >/dev/null; then
      ok "hbase -> us $enc/$codec ($(grep -c . <"$WORK/ref.txt") lines)"
    else
      ko "hbase -> us $enc/$codec differs"
      diff "$WORK/ref.txt" "$WORK/ours.txt" | head -4
    fi
  done
done

echo "== hfile-rs -> HBase"
CELFILE=$WORK/cells.parquet
if [ -z "$REFERENCE" ] || ! "$HFILE_BIN" hfile2arrow "$REFERENCE" --out "$CELFILE" >/dev/null; then
  echo "skipping: no reference file could be converted to Parquet"
else
  for enc in NONE PREFIX ROW_INDEX_V1; do
    for codec in $CROSSCHECK_CODECS; do
      file=$WORK/rs_${enc}_${codec}.hfile
      if ! "$HFILE_BIN" arrow2hfile "$CELFILE" --out "$file" --encoding "$enc" \
          --codec "$codec" --zstd-framing "$ZSTD_FRAMING" >/dev/null \
          2>"$WORK/rsw.err"; then
        ko "our write $enc/$codec: $(tail -1 "$WORK/rsw.err")"
        continue
      fi
      if ! $JAVA DumpHFile "$file" >"$WORK/hbasedump.txt" 2>"$WORK/hbasedump.err"; then
        ko "hbase read of our $enc/$codec: $(tail -1 "$WORK/hbasedump.err")"
        continue
      fi
      "$HFILE_BIN" dump "$file" >"$WORK/ourdump.txt" 2>&1
      if diff -q "$WORK/hbasedump.txt" "$WORK/ourdump.txt" >/dev/null; then
        ok "us -> hbase $enc/$codec ($(grep -c . <"$WORK/hbasedump.txt") lines)"
      else
        ko "us -> hbase $enc/$codec differs"
        diff "$WORK/hbasedump.txt" "$WORK/ourdump.txt" | head -4
      fi
    done
  done
fi

LASTROW=$(printf "row%06d" $((CROSSCHECK_ROWS - 1)))
NEXTROW=$(printf "row%06d" "$CROSSCHECK_ROWS")
# Present rows, one past the end, and rows that sort before the file.
BLOOMROWS="row000000 row000001 $LASTROW $NEXTROW row999999 row000000x zzz"

echo "== bloom filter"
if $JAVA WriteBloomHFile "$WORK/hbase_bloom.hfile" "$CROSSCHECK_ROWS" NONE \
    >/dev/null 2>"$WORK/bw.err"; then
  if $JAVA BloomCheck "$WORK/hbase_bloom.hfile" $BLOOMROWS >"$WORK/hb_bloom.txt" \
      2>"$WORK/hb_bloom.err" && "$HFILE_BIN" bloom "$WORK/hbase_bloom.hfile" $BLOOMROWS \
      >"$WORK/our_bloom.txt" 2>/dev/null; then
    if diff -q "$WORK/hb_bloom.txt" "$WORK/our_bloom.txt" >/dev/null; then
      ok "hbase -> us bloom ($(grep -c . "$WORK/hb_bloom.txt") rows, \
$(grep -c ' maybe$' "$WORK/hb_bloom.txt") maybe)"
    else
      ko "hbase -> us bloom differs"
      diff "$WORK/hb_bloom.txt" "$WORK/our_bloom.txt" | head -4
    fi
  else
    ko "hbase -> us bloom: $(tail -1 "$WORK/hb_bloom.err")"
  fi
  if [ -f "$CELFILE" ]; then
    if "$HFILE_BIN" arrow2hfile "$CELFILE" --out "$WORK/rs_bloom.hfile" --bloom ROW \
        --codec none >/dev/null 2>&1 && $JAVA BloomCheck "$WORK/rs_bloom.hfile" $BLOOMROWS \
        >"$WORK/hb_bloom2.txt" 2>"$WORK/hb_bloom2.err" && "$HFILE_BIN" bloom \
        "$WORK/rs_bloom.hfile" $BLOOMROWS >"$WORK/our_bloom2.txt" 2>/dev/null; then
      if diff -q "$WORK/hb_bloom2.txt" "$WORK/our_bloom2.txt" >/dev/null; then
        ok "us -> hbase bloom"
      else
        ko "us -> hbase bloom differs"
        diff "$WORK/hb_bloom2.txt" "$WORK/our_bloom2.txt" | head -4
      fi
    else
      ko "us -> hbase bloom: $(tail -1 "$WORK/hb_bloom2.err")"
    fi
  fi
else
  ko "hbase bloom write: $(tail -1 "$WORK/bw.err")"
fi

# Salted, big endian row keys: nine bytes, so the last byte is a murmur tail, and half of them
# have the high bit set. A hash that zero extends the tail puts the rows in different bits than
# HBase did and would answer "no" for rows that are in the file.
hexrow() { printf 'hex:%02x%016x' "$(( $1 & 0xff ))" "$(( $1 & 0xff ))"; }
BINROWS=""
for i in 0 1 2 0x7f 0x80 0x81 0xa5 0xc0 0xfe 0xff 70000 70001; do
  BINROWS="$BINROWS $(hexrow "$i")"
done
echo "== bloom filter, binary row keys"
if $JAVA WriteBloomHFile "$WORK/hbase_binbloom.hfile" 200 NONE binary >/dev/null \
    2>"$WORK/bwb.err" && $JAVA BloomCheck "$WORK/hbase_binbloom.hfile" $BINROWS \
    >"$WORK/hb_binbloom.txt" 2>"$WORK/hb_binbloom.err" && "$HFILE_BIN" bloom \
    "$WORK/hbase_binbloom.hfile" $BINROWS >"$WORK/our_binbloom.txt" 2>/dev/null; then
  if diff -q "$WORK/hb_binbloom.txt" "$WORK/our_binbloom.txt" >/dev/null; then
    ok "hbase -> us bloom, signed tails ($(grep -c ' maybe$' "$WORK/hb_binbloom.txt") maybe)"
  else
    ko "hbase -> us bloom, signed tails differ"
    diff "$WORK/hb_binbloom.txt" "$WORK/our_binbloom.txt" | head -4
  fi
else
  ko "hbase -> us bloom, signed tails: $(tail -1 "$WORK/hb_binbloom.err")"
fi

echo "== meta blocks"
if $JAVA WriteMetaHFile "$WORK/hbase_meta.hfile" "$CROSSCHECK_ROWS" >/dev/null 2>&1; then
  names=$("$HFILE_BIN" info "$WORK/hbase_meta.hfile" 2>/dev/null | grep "meta blocks")
  first=$("$HFILE_BIN" meta "$WORK/hbase_meta.hfile" meta-a 2>&1)
  second=$("$HFILE_BIN" meta "$WORK/hbase_meta.hfile" meta-b 2>&1)
  if [ "$first" = "hello-meta" ] && [ "$second" = "second block" ]; then
    ok "hbase -> us meta blocks $names"
  else
    ko "hbase -> us meta blocks: '$first' / '$second' $names"
  fi
fi
if [ -f "$CELFILE" ] && "$HFILE_BIN" arrow2hfile "$CELFILE" --out "$WORK/rs_meta.hfile" \
    --meta meta-a=hello-meta --meta "meta-b=second block" >/dev/null 2>&1; then
  ref=$($JAVA MetaBlockCheck "$WORK/rs_meta.hfile" meta-a meta-b 2>/dev/null | grep '^meta-' \
    | tr '\n' '|')
  if [ "$ref" = "meta-a hello-meta|meta-b second block|" ]; then
    ok "us -> hbase meta blocks"
  else
    ko "us -> hbase meta blocks: $ref"
  fi
fi

echo
echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
