#!/usr/bin/bash
ps aux | grep exchange_coindata_importer | tr -s " " | cut -d " " -f 2 | xargs kill -9
