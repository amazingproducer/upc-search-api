#!/bin/bash
wget -O- $1 | bsdtar -xof- | mongoimport
