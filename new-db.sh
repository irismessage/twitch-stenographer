#!/bin/bash
set -eux

working='archive.db'
destdir='archive'

mkdir -p "${destdir}"
timestamp=$(date '+%Y%m%dT%H%M%SZ')
bsdtar -a -cf "${destdir}/archive-${timestamp}.db.xz" "${working}"
rm "${working}"
