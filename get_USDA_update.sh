#!/bin/bash
wget -O- $1 | bsdtar -xf- -T USDA_FILES
