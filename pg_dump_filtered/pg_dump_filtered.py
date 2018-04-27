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
# Description: PG filtered dump export service.
import logging
import psycopg2
from urllib.parse import urlparse
from typing import List

from pg_dump_filtered.helpers import SchemaUtils, RequestBuilder, DumpBuilder

class PgDumpFiltered():
    """
    Service to generate databases dump an intermediary advanced select request based on database schema.
    """

    def __init__(
            self,
            db_uri: str=None,
            db_conn: psycopg2.extensions.connection=None,
            ignored_constraints: List[str]=[],
            sql_filters: str=None,
            dump_file_path: str="dump.sql"):
        """
        Initiate a dump filtered export service with a database.

        :param db_uri: Postgres Database URI, not needed if db_conn is provided.
        :param db_conn: psycopg2 connexion.
        :param ignored_constraints: Constraints names that should be ignored, this property can  be set after initialisation also.
        :param sql_filters: SQL filter chain, this property can  be set after initialisation also.
        :param dump_file_path: Path of the dump file (data will be written there). Default: dump.sql
        """
        self.logger = logging.getLogger(__name__)

        self._db_conn = db_conn if db_conn is not None else self._make_db_con_from_uri(db_uri=db_uri)
        self._sql_filters = sql_filters
        self._ignored_constraints = ignored_constraints
        self.dump_file_path = dump_file_path

        # helpers
        self._request_builder = None  # Lazy instanciation
        self.schema_utils = SchemaUtils(conn=self._db_conn, ignored_constraints=self.ignored_constraints)

        # values from helpers
        self._join_req = None   # JOINS of the requests
        self._select_reqs = None    # Generated selects of the request

    def _make_db_con_from_uri(self, db_uri: str) -> psycopg2.extensions.connection:
        """
        Create database connexion from URI.

        :param db_uri: Databse URI, for instance : postgresql://pg_dump_test:pg_dump_test@localhost:5432/pg_dump_test
        :return: Psycopg2 connexion.
        """
        self.logger.debug("Making connexion from an URI : %s", db_uri)
        db_uri_parsed = urlparse(db_uri)
        return psycopg2.connect(
            database=db_uri_parsed.path[1:],
            user=db_uri_parsed.username,
            password=db_uri_parsed.password,
            host=db_uri_parsed.hostname)

    @property
    def sql_filters(self) -> str:
        """
        Filters that will be applied on requests.
        """
        return self._sql_filters

    @sql_filters.setter
    def sql_filters(self, sql_filters: str):
        """
        Set filters that will be applied on requests.

        :param sql_filters: Filters that will be applied on requests in SQL format.
        """
        self.logger.debug("Filters that will be applied on resquest are : %s", sql_filters)
        self._sql_filters = sql_filters

    @property
    def ignored_constraints(self) -> List[str]:
        """
        List of constraints names that will be ignored in JOINS for the dump.
        You should set it if you have optional foreign keys or circular references.
        """
        return self._ignored_constraints

    @ignored_constraints.setter
    def ignored_constraints(self, ignored_constraints: List[str]):
        """
        Set ignored_constraints, these constraints will be ignore in JOINS for the dump.
        You should set it if you have optional foreign keys or circular references.

        :param ignored_constraints: List of constraints names that will be ignored.
        """
        self.logger.debug("Constraints that will be ignored : %s", ignored_constraints)
        self._ignored_constraints = ignored_constraints
        self._schema_utils = SchemaUtils(conn=self._db_conn, ignored_constraints=self._ignored_constraints)

    @property
    def schema_utils(self) -> SchemaUtils:
        """
        Schema helper.
        """
        if self._schema_utils is None:  # Lazy instanciation
            self.schema_utils = SchemaUtils(conn=self._db_conn, ignored_constraints=self.ignored_constraints)
        return self._schema_utils

    @schema_utils.setter
    def schema_utils(self, schema_utils):
        """
        Update schema utils and it's dependencies.
        """
        self.logger.debug("Updating schema_utils")
        self._schema_utils = schema_utils
        self.request_builder = None   # Unsetting dependent data

    @property
    def request_builder(self) -> RequestBuilder:
        """
        Request builder.
        """
        if self._request_builder is None:   # Lazy instanciation
            self._request_builder = RequestBuilder(schema_utils=self.schema_utils)
        return self._request_builder

    @request_builder.setter
    def request_builder(self, request_builder):
        """
        Setting request builder and unset dependent helpers/datas.
        """
        self._request_builder = request_builder

    def dump(self, tables_to_export: List[str]):
        """
        Dump some tables and all related datas. Dump to the output file directly.

        :param table_to_export: List of tables names that needs to be exported and all their related tables.
        """
        tables_to_request = self.schema_utils.list_all_related_tables(table_names=tables_to_export)

        self.logger.debug("Table to request : %r", tables_to_request)

        from_table_name = tables_to_export[0]  # Table that will be used in the FROM statment

        # Generating all JOINs, they aren't selective as it would be too difficult to draw a graph of the relations to determine if JOIN is needed or not
        join_req = self.request_builder.generate_join_statments(table_names=tables_to_request, exclude_from_statment=[from_table_name])
        self.logger.debug("Join request : %s", join_req)

        # generating select statements
        selects = self.request_builder.generate_all_select_statements(
            table_to_be_exported=tables_to_request,
            from_table_name=from_table_name,
            join_statements=join_req,
            where_filter=self.sql_filters)

        # Dumping datas
        self.logger.debug("Start dumping datas to : %s", self.dump_file_path)
        with open(self.dump_file_path, 'w') as dump_file:
            dump_builder = DumpBuilder(schema_utils=self.schema_utils, conn=self._db_conn, dump_file=dump_file)
            dump_builder.generate_all_delete_statements(
                from_table_name=from_table_name,
                table_to_be_exported=tables_to_request,
                join_statements=join_req,
                where_filter=self.sql_filters)
            dump_builder.dump_tables(select_requests=selects)

    def close(self):
        """
        Terminates all transactions closes database connexion.
        """
        self.logger.debug("Closing database connexion")
        self._db_conn.close()
