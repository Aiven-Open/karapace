#!/usr/bin/env bash

missing_copyright=$(
    grep \
        --extended-regexp \
        --files-without-match \
        --max-count=1 \
        'Copyright \(c\) 20[0-9]{2} Aiven' \
        -- "$@"
)

if [[ -n $missing_copyright ]]; then
    echo "💥 There are files missing required copyright statement."
    echo "$missing_copyright"
    exit 1
else
    echo "✅ All files have required copyright statement."
fi
