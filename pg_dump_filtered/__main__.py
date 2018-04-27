#!/usr/bin/python3
# coding: utf-8

# Copyright (C) 2017 Open Path View, Maison Du Libre
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <http://www.gnu.org/licenses/>.

# Contributors: Benjamin BERNARD <benjamin.bernard@openpathview.fr>
# Email: team@openpathview.fr
# Description: Export endpoint.

""" Postgres partial export.
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
    --debug                    Set logs to debug.
"""

import logging
from docopt import docopt
from path import Path

from pg_dump_filtered import PgDumpFiltered

MODULE_NAME = 'pg-dump-filtered'

# --- Logs // Console only
formatter_c = logging.Formatter('%(name)-30s: %(levelname)-8s %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter_c)
rootLogger = logging.getLogger()
rootLogger.addHandler(ch)

logger = logging.getLogger(__name__.split(".")[0])

def main():
    args = docopt(__doc__)

    logger.setLevel(logging.DEBUG if "--debug" in args and args["--debug"] else logging.INFO)
    logger.info("INFO")
    logger.debug("DEBUG")

    # Handling arguments
    db_uri_parsed = args["<db-uri>"]
    tables_to_export = args["<table-list>"].split(",")
    sql_filters = args["--filters"]
    ignored_constraints = args["--ignored-constraints"].split(",") if args["--ignored-constraints"] is not None else []
    ouput_file = Path(args["--output"])

    dump_service = PgDumpFiltered(
        db_uri=db_uri_parsed,
        ignored_constraints=ignored_constraints,
        sql_filters=sql_filters,
        dump_file_path=ouput_file)
    dump_service.dump(tables_to_export=tables_to_export)

    dump_service.close()

if __name__ == "__main__":
    main()
