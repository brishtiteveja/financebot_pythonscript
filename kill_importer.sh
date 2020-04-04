#!/usr/bin/bash
ps aux | grep importer_ccxt | cut -d " " -f 5 | xargs kill -9
