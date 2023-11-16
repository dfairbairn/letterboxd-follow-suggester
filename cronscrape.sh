#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Running in $SCRIPT_DIR"

source venv/bin/activate

TSTAMP=`date +'%Y%m%d_%H%M%S'`

python3 scrape.py -upd 2>&1 > logs/scrape_$TSTAMP.log &
