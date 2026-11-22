/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.utils.IOUtils;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Map;

/**
 * The Iceberg catalogs and tables that the deletion vector preparers of a TaskManager share: one
 * catalog per catalog configuration, and one handle per table, closed once no preparer holds them.
 *
 * <p>A preparer lives as long as its tiering source reader, and a catalog may hold connections for
 * its whole life (a JDBC catalog keeps a pool of them). A catalog of its own per preparer holds
 * connections for each reader and table, which can exhaust the catalog database's.
 */
final class SharedIcebergTables {

    /** The catalogs and tables of this TaskManager's preparers. */
    static final SharedIcebergTables INSTANCE = new SharedIcebergTables();

    // by catalog configuration; guarded by this
    private final Map<Map<String, String>, SharedCatalog> catalogs = new HashMap<>();

    /**
     * Holds the table, loaded with a catalog of the provider's configuration, which is created if
     * no handle holds one.
     */
    synchronized Handle acquire(IcebergCatalogProvider provider, TableIdentifier identifier) {
        Map<String, String> key = provider.catalogProperties();
        SharedCatalog catalog = catalogs.get(key);
        if (catalog == null) {
            catalog = new SharedCatalog(provider.get());
            catalogs.put(key, catalog);
        }
        SharedTable table = catalog.tables.get(identifier);
        if (table == null) {
            try {
                table = new SharedTable(catalog.catalog.loadTable(identifier));
            } catch (RuntimeException e) {
                closeIfUnused(key, catalog);
                throw e;
            }
            catalog.tables.put(identifier, table);
        }
        table.holders++;
        return new Handle(key, identifier, table.table);
    }

    /** The catalogs held open. */
    @VisibleForTesting
    synchronized int openCatalogs() {
        return catalogs.size();
    }

    private synchronized void release(Handle handle) {
        SharedCatalog catalog = catalogs.get(handle.catalogKey);
        SharedTable table = catalog.tables.get(handle.identifier);
        if (--table.holders == 0) {
            catalog.tables.remove(handle.identifier);
            closeIfUnused(handle.catalogKey, catalog);
        }
    }

    private void closeIfUnused(Map<String, String> key, SharedCatalog catalog) {
        if (catalog.tables.isEmpty()) {
            catalogs.remove(key);
            if (catalog.catalog instanceof AutoCloseable) {
                IOUtils.closeQuietly((AutoCloseable) catalog.catalog, "iceberg-catalog");
            }
        }
    }

    /** A table a preparer holds; closing the handle releases it. */
    final class Handle implements AutoCloseable {
        private final Map<String, String> catalogKey;
        private final TableIdentifier identifier;
        private final Table table;
        // guarded by the enclosing SharedIcebergTables
        private boolean released;

        private Handle(Map<String, String> catalogKey, TableIdentifier identifier, Table table) {
            this.catalogKey = catalogKey;
            this.identifier = identifier;
            this.table = table;
        }

        /** The table, which the preparers of the TaskManager share. */
        Table table() {
            return table;
        }

        @Override
        public void close() {
            synchronized (SharedIcebergTables.this) {
                if (!released) {
                    released = true;
                    release(this);
                }
            }
        }
    }

    private static final class SharedCatalog {
        final Catalog catalog;
        final Map<TableIdentifier, SharedTable> tables = new HashMap<>();

        SharedCatalog(Catalog catalog) {
            this.catalog = catalog;
        }
    }

    private static final class SharedTable {
        final Table table;
        int holders;

        SharedTable(Table table) {
            this.table = table;
        }
    }
}
