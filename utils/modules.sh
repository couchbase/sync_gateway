#!/bin/sh
browserify -r coax -r ./assets/syncModel.js:syncModel > ./assets/modules.js
