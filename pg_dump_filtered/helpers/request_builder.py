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
# Description: Utils function to generate SQL request.

import logging
from typing import List, Dict
from pg_dump_filtered.helpers import SchemaUtils

class RequestBuilder():

    def __init__(self, schema_utils: SchemaUtils):
        """
        Instanciate a request builder.

        :param schema_utils: Schema utils used to fetch some related schema informations.
        """
        self._schema_utils = schema_utils
        self.logger = logging.getLogger(__name__)

    def generate_join_statments(self, table_names: List[str], exclude_from_statment: List[str]=[]) -> str:
        """
        Generate a general JOIN statement with all necessary JOINS for listed tables based on ther foreign_key.
        For nullable foreign_keys we use LEFT JOIN othewise INNER JOIN.

        :param table_names: List of tables that should be involved in the JOIN statment.
        :param exclude_from_statment: List of table that will be excluded, typically you need to exclude the table used in FROM statement.
        :return: full JOIN request.
        """
        self.logger.debug("Generating join statement for tables: %r, excluding: %r", table_names, exclude_from_statment)
        join_req = ""
        for table_name in table_names:
            fks = self._schema_utils.fetch_foreign_keys(table_name=table_name)

            for fk in fks.values():
                join_rules = []
                fk_referenced_table = None
                as_nullable_field = False
                for col_constraint in fk.matching_columns:
                    join_rules.append(
                        "{foreign.table_name}.{foreign.column_name} = {referenced.table_name}.{referenced.column_name}"
                        .format(foreign=col_constraint.foreign_col, referenced=col_constraint.referenced_col)
                    )
                    fk_referenced_table = col_constraint.referenced_col.table_name

                    # if it's a composite key all fields will be nullable
                    as_nullable_field = self._schema_utils.is_nullable(column=col_constraint.foreign_col)

                if fk_referenced_table in exclude_from_statment:
                    continue

                fk_req_rules = " AND ".join(join_rules)

                # Using LEFT JOIN when the foreign key is nullable, to prevent unecessary restriction if the key is null
                fk_join_type = "LEFT" if as_nullable_field else "INNER"

                fk_join_req = "{join_type} JOIN {referenced_table} ON {rules} ".format(
                    join_type=fk_join_type,
                    referenced_table=fk_referenced_table,
                    rules=fk_req_rules
                )
                join_req += "\n" + fk_join_req

        return join_req

    def generate_select_statement(self, from_table_name: str, displayed_fields_table_name: str, join_statements: str, where_filter: str="") -> str:
        """
        Generate a select statement.
        """
        where = "" if where_filter == "" or where_filter is None else " WHERE " + where_filter

        req = """SELECT DISTINCT {displayed_fields_table_name}.* FROM {from_table_name} {join_statements} {where} """.format(
            displayed_fields_table_name=displayed_fields_table_name,
            from_table_name=from_table_name,
            join_statements=join_statements,
            where=where)
        return req

    def generate_all_select_statements(self, table_to_be_exported: List[str], from_table_name: str, join_statements: str, where_filter: str="") -> Dict[str, str]:
        """
        Generate all select statements.

        :param table_to_be_exported: Tables that will be exported.
        :param from_table_name: Table used in the FROM statement, should not be mentionned in the JOIN statements.
        :param where_filter: SQL filters.
        :return: Dictionnary of select statement for each table (table_name => select statement)
        """
        self.logger.debug("Generating all select statements for tables : %r, FROM table is: %r", table_to_be_exported, from_table_name)
        self.logger.debug("Used filters : %s", where_filter)
        select_requests = {}
        for tname in table_to_be_exported:
            self.logger.debug("#### Request for %r ####", tname)
            req = self.generate_select_statement(
                from_table_name=from_table_name,
                displayed_fields_table_name=tname,
                join_statements=join_statements,
                where_filter=where_filter)
            select_requests[tname] = req
            self.logger.debug(req)

        return select_requests
