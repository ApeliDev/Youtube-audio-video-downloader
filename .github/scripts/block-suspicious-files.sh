#!/usr/bin/env bash
# Fails the job if it finds the malware patterns found in this org's repos
# in Nov 2026: a huge whitespace-padded obfuscated tail appended to an
# otherwise legitimate file, a whole file that is nothing but an obfuscated
# payload (sometimes disguised with a non-code extension, e.g. a "font"
# named fa-brand-400.orc), or a `node <bareword> &&` dropper wired into
# package.json scripts ahead of the real build command.
set -uo pipefail
fail=0

while IFS= read -r -d '' f; do
  grep -qI . "$f" 2>/dev/null || continue
  if perl -ne '
       if (/[ \t]{100,}(\S.*)$/) {
         my $t = $1;
         my $hexn = () = $t =~ /0x[0-9a-fA-F]+/g;
         if ($hexn >= 15 || $t =~ /eval|atob|Function\(|global[\[.]|fromCharCode|_0x[0-9a-f]{4}|require\(|base64|\\x[0-9a-f]{2}|global\s*,\s*require|require\s*,\s*global/i) {
           print "1"; last;
         }
       }
     ' "$f" 2>/dev/null | grep -q 1; then
    echo "::error file=$f::suspicious obfuscated tail appended to this file"
    fail=1
  fi
  size=$(wc -c < "$f" 2>/dev/null || echo 0)
  lines=$(wc -l < "$f" 2>/dev/null || echo 0)
  if [ "$size" -gt 3000 ] && [ "$lines" -le 5 ]; then
    hexcount=$(grep -oE '0x[0-9a-fA-F]+' "$f" 2>/dev/null | wc -l)
    if [ "$hexcount" -ge 20 ] || grep -qE 'global[, ]*require|require[, ]*global|_0x[0-9a-f]{4}' "$f" 2>/dev/null; then
      echo "::error file=$f::whole file looks like an obfuscated payload"
      fail=1
    fi
  fi
done < <(find . \( -path '*/node_modules/*' -o -path '*/vendor/*' -o -path './.git/*' \
               -o -path '*/dist/*' -o -path '*/build/*' -o -path '*/.next/*' -o -path '*/storage/*' \) \
             -prune -o -type f -print0)

if [ -f package.json ] && grep -qE '"(dev|build|start|postinstall|preinstall|prepare)" *: *"[^"]*\bnode +[A-Za-z0-9_-]+( |&&)' package.json; then
  echo "::error file=package.json::a script runs a bare 'node <file>' before the real build command — verify this is not a dropper"
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "Blocking: suspicious file(s) detected. See annotations above."
  exit 1
fi
echo "No suspicious files detected."
