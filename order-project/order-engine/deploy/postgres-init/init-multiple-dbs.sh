#!/bin/bash
# Postgres imajÄ±nÄ±n resmi desteklemediÄŸi Ã§oklu-database oluÅŸturma script'i.
# POSTGRES_MULTIPLE_DATABASES env deÄŸiÅŸkenindeki virgÃ¼lle ayrÄ±lmÄ±ÅŸ isimleri
# ilk container baÅŸlangÄ±cÄ±nda otomatik olarak oluÅŸturur (Database-per-Service
# pattern: orders, inventory, payments birbirinden izole).
set -e
set -u

function create_database() {
	local database=$1
	echo "  -> '$database' veritabanÄ± oluÅŸturuluyor"
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
	    SELECT 'CREATE DATABASE "$database"'
	    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
EOSQL
}

if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
	echo "Birden fazla veritabanÄ± oluÅŸturuluyor: $POSTGRES_MULTIPLE_DATABASES"
	IFS=',' read -ra DBS <<< "$POSTGRES_MULTIPLE_DATABASES"
	for db in "${DBS[@]}"; do
		create_database "$db"
	done
	echo "VeritabanlarÄ± oluÅŸturuldu."
fi
