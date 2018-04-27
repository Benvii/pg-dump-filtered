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
# Description: Helps you extract information from standard information_schema database.

import logging
import psycopg2
import psycopg2.extras

from typing import List
from pg_dump_filtered import model

REQ_FOREING_KEY_FOR_A_TABLE = """
    SELECT
         KCU1.CONSTRAINT_NAME AS fk_constraint_name
        ,KCU1.TABLE_NAME AS fk_table_name
        ,KCU1.COLUMN_NAME AS fk_column_name
        ,KCU2.TABLE_NAME AS referenced_table_name
        ,KCU2.COLUMN_NAME AS referenced_column_name
    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC

    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1
        ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
        AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
        AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME

    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2
        ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG
        AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA
        AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
        AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION

    WHERE KCU1.TABLE_NAME = '{table_name}';
    """

REQ_IS_NULLABLE = """
    SELECT is_nullable
    FROM information_schema.columns
    WHERE table_name = '{table_name}' AND column_name = '{column_name}';
"""

REQ_FETCH_COLULMNS = """
    SELECT COLUMN_NAME FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ORDINAL_POSITION;
"""

REQ_FETCH_TABLE_PRIMARY_KEYS = """
SELECT kc.column_name
FROM
    information_schema.table_constraints tc,
    information_schema.key_column_usage kc
WHERE
    tc.constraint_type = 'PRIMARY KEY'
    and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
    and kc.constraint_name = tc.constraint_name
    and kc.table_name = '{table_name}';
"""

class SchemaUtils():
    """
    Helps you extract informations from information_schema database.
    """

    def __init__(self, conn: psycopg2.extensions.connection, ignored_constraints: List[str]=[]):
        """
        Intanciate a SchemaUtils class.

        :param conn: The psycog connexion.
        """
        self.logger = logging.getLogger(__name__)

        self.logger.debug("Instanciating utils with conn : %r", conn)
        self.conn = conn
        self._ignored_constraints = ignored_constraints
        self._fk_cache = {}  # cache of foreign keys "tablename": Dict[str, model.ForeignKey]
        self._is_nullable_cache = {}  # cache of is_nullable request, "table_name.columname" -> bool

    def _map_foreign_key_to_model(self, db_row: List[str]):
        """
        Map a foreign key.

        :param db_row: Entry of the database, field correspond to REQ_FOREING_KEY_FOR_A_TABLE.
        :return: A foreign_key model.
        """
        match_cols = []
        match_cols.append(
            model.ColumnConstraint(
                foreign_col=model.ColumnRef(
                    table_name=db_row['fk_table_name'],
                    column_name=db_row['fk_column_name']),
                referenced_col=model.ColumnRef(
                    table_name=db_row['referenced_table_name'],
                    column_name=db_row['referenced_column_name'])
            )
        )
        return model.ForeignKey(constraint_name=db_row['fk_constraint_name'], matching_columns=match_cols)

    def fetch_foreign_keys(self, table_name: str) -> List[model.ForeignKey]:
        """
        Fecth all foreign keys constraint for a specified table.

        :param table_name: Will fetch the foreign keys of this table.
        :return: Return a list of foreign_keys.
        """
        self.logger.debug("Fetching foreign keys for table: %s", table_name)

        if table_name in self._fk_cache:
            self.logger.debug("Already in cache : %r", self._fk_cache[table_name])
            return self._fk_cache[table_name]

        constraints = {}
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(REQ_FOREING_KEY_FOR_A_TABLE.format(table_name=table_name))

        for row in cur:
            fk = self._map_foreign_key_to_model(row)

            if fk.constraint_name in self._ignored_constraints:
                continue

            if fk.constraint_name in constraints:  # if constraint with this name already exists (for grouped keys)
                constraints[fk.constraint_name].matching_columns.extend(fk.matching_columns)
            else:
                constraints[fk.constraint_name] = fk

        self.logger.debug("Fetched constraints : %r", constraints)
        self._fk_cache[table_name] = constraints

        return constraints

    def fetch_primary_keys(self, table_name: str) -> List[model.ColumnRef]:
        """
        Fetch table primary keys.

        :param table_name: Primary keys of this table will be extracted.
        :return: List of column that are primary keys.
        """
        self.logger.debug("Fetching primary keys for : %s", table_name)
        keys = []

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(REQ_FETCH_TABLE_PRIMARY_KEYS.format(table_name=table_name))

        for row in cur:
            if "column_name" in row:
                keys.append(model.ColumnRef(table_name=table_name, column_name=row["column_name"]))

        self.logger.debug("Found theses primary keys : %r", keys)
        return keys

    def list_all_related_tables(self, table_names: List[str]) -> List[str]:
        """
        Fetch all tables names related (tables referenced by the tables in table_names) to the tables in parameter.

        :param table_names: Table names that we be used to search all dependencies.
        :return: List of tables containing table_names and the tables that referenced them (recurively).
        """
        self.logger.debug("Listing tables referencing those tables : %r", table_names)
        treated = []
        tables_to_treat = []
        tables_to_treat.extend(table_names)

        while tables_to_treat != []:
            tname = tables_to_treat.pop()

            if tname in treated:
                continue

            treated.append(tname)
            fks = self.fetch_foreign_keys(table_name=tname)

            for fk in fks.values():
                for col_constraint in fk.matching_columns:
                    ref_tname = col_constraint.referenced_col.table_name
                    self.logger.debug("%s <- %s", tname, ref_tname)
                    if not(ref_tname in treated):
                        tables_to_treat.append(ref_tname)

        self.logger.debug("Related tables are : %r", treated)
        return treated

    def is_nullable(self, column: model.ColumnRef) -> bool:
        """
        Return true if a column is nullabmle. A cache is also used to limit requests.

        :param column: The column.
        :return: True if it's nullable.
        """
        self.logger.debug("is_nullable : %r", column)

        key = "{c.table_name}.{c.column_name}".format(c=column)
        if key in self._is_nullable_cache:
            self.logger.debug("Found in cache : %r", self._is_nullable_cache[key])
            return self._is_nullable_cache[key]

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(REQ_IS_NULLABLE.format(table_name=column.table_name, column_name=column.column_name))
        for row in cur:
            nullable = row['is_nullable'] == "YES"
            self._is_nullable_cache[key] = nullable
            return nullable

    def fetch_cols_names(self, table_name: str) -> List[model.ColumnRef]:
        """
        Gets table columns in ORDINAL_POSITION order.

        :param table_name: Table where columns names will be extracted.
        :return: The list of columnsRef.
        """
        self.logger.debug("fetch_cols_names for table %r", table_name)
        cols = []

        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(REQ_FETCH_COLULMNS.format(table_name=table_name))

        for row in cur:
            if "column_name" in row:
                cols.append(model.ColumnRef(table_name=table_name, column_name=row["column_name"]))

        self.logger.debug("Found cols : %r", cols)
        return cols
