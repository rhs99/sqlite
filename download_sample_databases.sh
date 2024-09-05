#!/bin/sh

echo "Downloading superheroes db"
curl -Lo superheroes.db https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/superheroes.db

echo "Downloading companies db"
curl -Lo companies.db https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/companies.db

echo "Sample databases downloaded."
