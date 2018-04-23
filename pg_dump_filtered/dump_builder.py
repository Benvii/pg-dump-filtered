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
# Description: Utils function to generate SQL/PG dump.

import logging
import psycopg2
import psycopg2.extras
from typing import TextIO
from pg_dump_filtered import SchemaUtils

REQ_SELECT_DUMP = "COPY ({select}) TO STDOUT"

DP_HEADER = """--
-- PostgreSQL database dump
--

-- Dumped from pg_filtered python script

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
"""
DP_COPY_HEADER = """
-- Table : {table_name}

COPY public.{table_name} ({cols_names}) FROM stdin;"""
DP_STDIN_END = "\.\n\n"

class DumpBuilder():

    def __init__(self, schema_utils: SchemaUtils, conn: psycopg2.extensions.connection, dump_file: TextIO):
        """
        Instanciate a request builder.

        :param schema_utils: Schema utils used to fetch some related schema informations.
        :param conn: database connexion.
        :param dump_file: File where the dump will be made.
        """
        self._schema_utils = schema_utils
        self._dump_file = dump_file
        self._conn = conn
        self.logger = logging.getLogger(__name__)

        self._dump_file.write(DP_HEADER)

    def dump(self, table_name: str, select_request: str):
        """
        Generate the COPY statement for a table from a select request.
        Dump result wil be appened to the export file.

        :param table_name: Table that will be dumped, used to key it's schema.
        :param select_request: Select request, which request the table's data.
        """
        self.logger.debug("dump for table_name: %s", table_name)

        # Making COPY header
        cols = self._schema_utils.fetch_cols_names(table_name=table_name)
        cols_names_list = ["\"{cname}\"".format(cname=c.column_name) for c in cols]  # prevent uppercases columns names errors
        cols_names = ", ".join(cols_names_list)
        header = DP_COPY_HEADER.format(table_name=table_name, cols_names=cols_names)
        self._dump_file.write(header + "\n")
        self.logger.debug("Generated COPY statement for dump file : %s", header)

        # Execute "dump" request based on select
        cur = self._conn.cursor()
        cur.copy_expert(REQ_SELECT_DUMP.format(select=select_request), self._dump_file)
        self._dump_file.write(DP_STDIN_END)
        self.logger.debug("Dump saved into dump_file")
