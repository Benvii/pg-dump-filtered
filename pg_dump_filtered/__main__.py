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
    -h --help                  Show this screen
    --debug                    Set logs to debug.
"""

import logging
import psycopg2
from docopt import docopt
from urllib.parse import urlparse

from pg_dump_filtered import SchemaUtils, RequestBuilder, model

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
    db_uri_parsed = urlparse(args["<db-uri>"])
    tables_to_export = args["<table-list>"].split(",")

    # database connexion
    db_conn = psycopg2.connect(
        database=db_uri_parsed.path[1:],
        user=db_uri_parsed.username,
        password=db_uri_parsed.password,
        host=db_uri_parsed.hostname)

    # getting related tables informations for schema
    schema_utils = SchemaUtils(conn=db_conn)
    request_builder = RequestBuilder(schema_utils=schema_utils)
    tables_to_request = schema_utils.list_all_related_tables(table_names=tables_to_export)

    from_table_name = tables_to_export[0]  # Table that will be used in the FROM statment

    # Generating all JOINs, they aren't selective as it would be too difficult to draw a graph of the relations to determine if JOIN is needed or not
    join_req = request_builder.generate_join_statments(table_names=tables_to_request, exclude_from_statment=[from_table_name])
    logger.debug("#### JOIN REQUEST FOR ALL TABLES ####")
    logger.debug(join_req)

    for tname in tables_to_request:
        logger.debug("#### Request for %r ####", tname)
        req = request_builder.generate_select_statement(from_table_name=from_table_name, displayed_fields_table_name=tname, join_statements=join_req)
        logger.debug(req)

        # filters TODO

    # schema_utils.fetch_foreign_keys(table_name="lot")
    # schema_utils.fetch_foreign_keys(table_name="lot")

    db_conn.close()

if __name__ == "__main__":
    main()
