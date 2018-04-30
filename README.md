# pg-dump-filtered - Postgres partial dump

This module was designed to do partial dump (filtered dump) on a set of tables taking in account
all their foreign_keys, so table they are referencing.
If you have a huge database and you want to extract a small set of data (with there dependencies in other tables) to work on it and them re-import the datas
this script is made for you.

Filtering is made using SQL where statement on a SELECT statement with all tables relations handeled as INNER or LEFT JOIN (depending on the nullability of the foreign_key).

The generated dump is a set of COPY statements with raw values, so that it can handle all type of values (dates, binary, postgis points ...).

## Installation
```bash
pip install -r requirements.txt
python setup.py install
```

## How to use it

### Command line interface

```bash
Usage:
    pg-dump-filtered [options] <db-uri> <table-list>

Arguments:
    db-uri                   URI of the postgres database, for instance : postgresql://pg_dump_test:pg_dump_test@localhost:5432/pg_dump_test
    table-list               List of the table that needs to be exported, separated by commas (related tables will automatically be exported).
                             Eg : 'table1,table2,table3'

Options:
    -h --help                  Show this screen.
    --filters=<SQL>            SQL filters. Eg: mytable.mycol = 'value' AND myothertable.toto LIKE 'titi'
    --ignored-constraints=<str>      List of constraints to be ignored. Eg : "myconstraint,myotherconstraint"
    --output=<str>             Dump file path. [default: dump.sql]
```

```bash
pg-dump-filtered "postgres://user:pwd@host/db" "tableA,tableB" --debug --filters="mytable.id=85 AND ....." --ignored-constraints="a_circular_constraint_name"
```

### Python Interface

For [Open Path View](https://openpathview.fr) whe needed to export small set a data depending on their geolocalisation and list some row of the exported datas (files UUID as files where saved in the database).

You can see how to use if in the [main CLI entry point](pg_dump_filtered/__main__.py).
