#!/bin/bash
rm -f products.bson && rm -f products.metadata.json && wget -O- https://static.openfoodfacts.org/data/openfoodfacts-mongodbdump.tar.gz | bsdtar -xof- --strip-components 2 && mongorestore -d off_temp -c product_info --drop products.bson && rm -f products.bson && rm -f products.metadata.json
