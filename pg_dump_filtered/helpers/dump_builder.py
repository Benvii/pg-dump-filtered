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
from typing import TextIO, List, Dict
from pg_dump_filtered.helpers import SchemaUtils

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


DP_DISABLE_TABLE_TRIGGERS = """
ALTER TABLE public.{table_name} DISABLE TRIGGER ALL;
"""

DP_ENABLE_TABLE_TRIGGERS = """
ALTER TABLE public.{table_name} ENABLE TRIGGER ALL;
"""

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

    def _enable_triggers(self, table_name: str):
        """
        Enable triggers for table_name.

        :param table_name: Table where triggers will be enabled.
        """
        self._dump_file.write(DP_ENABLE_TABLE_TRIGGERS.format(table_name=table_name))

    def _disable_triggers(self, table_name: str):
        """
        Disable triggers for table_name.

        :param table_name: Table where triggers will be disabled.
        """
        self._dump_file.write(DP_DISABLE_TABLE_TRIGGERS.format(table_name=table_name))

    def dump(self, table_name: str, select_request: str):
        """
        Generate the COPY statement for a table from a select request.
        Dump result wil be appened to the export file.

        :param table_name: Table that will be dumped, used to key it's schema.
        :param select_request: Select request, which request the table's data.
        """
        self.logger.debug("dump for table_name: %s", table_name)

        # disable triggers to prevent key relations errors (depending on COPY order in tables)
        self._disable_triggers(table_name)

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

        # Enabling triggers back
        self._enable_triggers(table_name)

        self.logger.debug("Dump saved into dump_file")

    def dump_tables(self, select_requests: Dict[str, str]):
        """
        Dump all data corresponding to select requests for each table_name.

        :param select_requests: Dictionnary associating table_name => select request.
        """
        self.logger.debug("Dumping mutiple tables : %r", select_requests.keys())

        for table_name, select in select_requests.items():
            self.dump(table_name=table_name, select_request=select)

    def generate_primary_keys_delete_statements(self, from_table_name: str, displayed_fields_table_name: str, join_statements: str, where_filter: str=""):
        """
        Will generate a delete statement for all selected datas.

        :param from_table_name: Table used in from statement.
        :param
        """
        self.logger.debug("Generating delation statements for table : %s", displayed_fields_table_name)
        self._dump_file.write("""-- delete statements for partial dump of table : {table_name}\n""".format(table_name=displayed_fields_table_name))
        pkeys_cols = self._schema_utils.fetch_primary_keys(table_name=displayed_fields_table_name)
        where = "" if where_filter == "" or where_filter is None else " WHERE " + where_filter

        # disabling triggers, ugly but can't do anything else
        # this is use to prevent cascade delation as our purpose is to update data not delete all related ones
        self._disable_triggers(displayed_fields_table_name)

        # select with pkeys cols to gets ids
        select_view = ", ".join([c.table_name + "." + c.column_name for c in pkeys_cols])
        select_keys_values_req = "SELECT {select_view} FROM {from_table_name} {join_statements} {where}".format(
            select_view=select_view, from_table_name=from_table_name, join_statements=join_statements, where=where)
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.logger.debug(select_keys_values_req)
        cur.execute(select_keys_values_req)
        for row in cur:
            delete_where = " AND ".join(["{tname}.{cname} = '{id}'".format(tname=c.table_name, cname=c.column_name, id=row[c.column_name]) for c in pkeys_cols])
            self._dump_file.write("DELETE FROM public.{table_name} WHERE {where}; \n".format(table_name=displayed_fields_table_name, where=delete_where))

        # Setting triggers back
        self._enable_triggers(displayed_fields_table_name)

        self._dump_file.write("\n")

    def generate_all_delete_statements(self, from_table_name: str, table_to_be_exported: List[str], join_statements: str, where_filter: str=""):
        """
        Generate all delete statements.

        :param table_to_be_exported: Tables that will be exported.
        :param from_table_name: Table used in the FROM statement, should not be mentionned in the JOIN statements.
        :param where_filter: SQL filters.
        :return: Dictionnary of select statement for each table (table_name => select statement)
        """
        self.logger.debug("Generating all delete statements for tables : %r, FROM table is: %r", table_to_be_exported, from_table_name)
        self.logger.debug("Used filters : %s", where_filter)
        for tname in table_to_be_exported:
            self.logger.debug("#### Dump for %r ####", tname)
            self.generate_primary_keys_delete_statements(
                from_table_name=from_table_name,
                displayed_fields_table_name=tname,
                join_statements=join_statements,
                where_filter=where_filter)
