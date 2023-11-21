#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR
echo "Running in $SCRIPT_DIR"

source venv/bin/activate

TSTAMP=`date +'%Y%m%d_%H%M%S'`

python3 scrape.py -upd 50 2>&1 > logs/scrape_$TSTAMP.log &
deactivate
