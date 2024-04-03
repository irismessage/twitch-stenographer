#!/bin/bash
set -eux

working='archive.db'

timestamp=$(date '+%Y%m%dT%H%M%SZ')
bsdtar -a -cf "archive/archive-${timestamp}.db.xz" "${working}"
rm "${working}"
