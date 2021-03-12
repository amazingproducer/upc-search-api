#!/bin/bash
rm -f branded_food.csv && rm -f food_category.csv && rm -f food.csv && wget -O- $1 | bsdtar -xf- -T USDA_FILES
