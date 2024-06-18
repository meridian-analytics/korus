#!/bin/bash
unset JUPYTER_PATH
unset JUPYTER_CONFIG_DIR

#set exit-on-error mode:
set -e 

# Run Tutorial 1
cd tutorial/t1
rm -rf tmp
mkdir tmp
cp taxonomy.ipynb tmp/
cd tmp/
cmd="jupyter nbconvert --to notebook --inplace --execute taxonomy.ipynb"
$cmd
FILE="tax_t1.sqlite"
if test -f "$FILE"; then
    echo "$FILE created"
else
    exit 1  #exit with Error
fi
cd ..
rm -rf tmp/
cd ../../

# Run Tutorial 2
cd tutorial/t2
rm -rf tmp
mkdir tmp
cp retrieve_data.ipynb tmp/
cp db_t3.sqlite tmp/
cd tmp/
cmd="jupyter nbconvert --to notebook --inplace --execute retrieve_data.ipynb"
$cmd
cd ..
rm -rf tmp/
cd ../../

# Run Tutorial 3
cd tutorial/t3
rm -rf tmp
mkdir tmp
cp add_data.ipynb tmp/
cp tax_t1.sqlite tmp/
cp -r data tmp/
cd tmp/
cmd="jupyter nbconvert --to notebook --inplace --execute add_data.ipynb"
$cmd
FILE="mydb.sqlite"
if test -f "$FILE"; then
    echo "$FILE created"
else
    exit 1  #exit with Error
fi
cd ..
rm -rf tmp/
cd ../../
