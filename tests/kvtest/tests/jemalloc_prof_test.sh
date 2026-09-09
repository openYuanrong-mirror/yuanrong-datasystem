#!/usr/bin/env bash
set -euo pipefail

binary="$(realpath "$1")"
expected="$2"
name="$(basename "$binary")"
mkdir -p "$TEST_TMPDIR"
stage="$(mktemp -d "${TEST_TMPDIR}/${name}-prof.XXXXXX")"
mkdir -p "$stage/lib" "$stage/logs/jemalloc"
cp "$binary" "$stage/$name"
cp -L "$3" "$stage/lib/libjemalloc.so.2"
unset LD_LIBRARY_PATH LD_PRELOAD MALLOC_CONF
LC_ALL=C LD_DEBUG=bindings LD_BIND_NOW=1 "$stage/$name" --version \
    > "$stage/version.out" 2> "$stage/bindings.log"
grep -x "jemalloc_prof_supported=$expected" "$stage/version.out"
readelf -d "$stage/$name" | grep 'libjemalloc.so.2'
for symbol in malloc free; do
    if ! grep -F "binding file $stage/$name [" "$stage/bindings.log" \
        | grep -F "to $stage/lib/libjemalloc.so.2 [" \
        | grep -F "symbol \`$symbol'" >/dev/null; then
        echo "ERROR: $name $symbol is not provided by the bundled jemalloc" >&2
        exit 1
    fi
done
if [[ "$expected" == true ]]; then
    MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:0,prof_final:true,prof_prefix:$stage/logs/jemalloc/$name" \
        "$stage/$name" --version
    compgen -G "$stage/logs/jemalloc/$name.*.heap" >/dev/null
fi
