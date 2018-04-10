#!/usr/bin/env bash

INPUT=input
mkdir -p ${INPUT}
rm -rf ${INPUT}/*
cd ${INPUT}
wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip ml-latest-small.zip
rm -f ml-latest-small.zip
mv ml-latest-small/* .
rmdir ml-latest-small
