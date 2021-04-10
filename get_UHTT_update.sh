#!/bin/bash
rm -f uhtt_barcode_ref_all.7z && wget https://github.com/papyrussolution/UhttBarcodeReference/releases/download/20200218/uhtt_barcode_ref_all.7z && 7zr e -aoa uhtt_barcode_ref_all.7z && rm -f uhtt_barcode_ref_all.7z
