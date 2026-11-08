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

package org.apache.fluss.lake.iceberg.testutils;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Hadoop catalog that runs a test's hook right before a table's next commit of a snapshot with a
 * given summary property, e.g. to commit another engine's write between a tiering round's base and
 * its commit. The commit itself then finds the table moved and retries, as Iceberg does. Set as
 * {@code catalog-impl}; hooks are static, so the tiering job's catalog sees the test's hooks.
 */
public class CommitHookCatalog extends HadoopCatalog {

    private static final Map<String, Hook> HOOKS = new ConcurrentHashMap<>();

    /** Runs before a commit of {@code pending} on top of {@code base}. */
    @FunctionalInterface
    public interface Hook {
        void beforeCommit(TableMetadata base, TableMetadata pending, FileIO io) throws Exception;
    }

    /**
     * Arms {@code hook} for the next commit of table {@code tableName} (e.g. {@code db.t}) whose
     * new snapshot has the summary property {@code property}; it runs once.
     */
    public static void armOnce(String tableName, String property, Hook hook) {
        HOOKS.put(
                tableName,
                new Hook() {
                    private boolean fired;

                    @Override
                    public synchronized void beforeCommit(
                            TableMetadata base, TableMetadata pending, FileIO io) throws Exception {
                        Snapshot snapshot = pending.currentSnapshot();
                        if (fired
                                || snapshot == null
                                || snapshot.summary() == null
                                || !snapshot.summary().containsKey(property)) {
                            return;
                        }
                        fired = true;
                        HOOKS.remove(tableName, this);
                        hook.beforeCommit(base, pending, io);
                    }
                });
    }

    /** Whether the hook armed for {@code tableName} has not run yet. */
    public static boolean isArmed(String tableName) {
        return HOOKS.containsKey(tableName);
    }

    public static void disarm(String tableName) {
        HOOKS.remove(tableName);
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier identifier) {
        TableOperations delegate = super.newTableOps(identifier);
        String tableName = identifier.namespace() + "." + identifier.name();
        return new TableOperations() {
            @Override
            public void commit(TableMetadata base, TableMetadata metadata) {
                Hook hook = HOOKS.get(tableName);
                if (hook != null) {
                    try {
                        hook.beforeCommit(base, metadata, delegate.io());
                    } catch (Exception e) {
                        throw new IllegalStateException(
                                "Commit hook of " + tableName + " failed", e);
                    }
                }
                delegate.commit(base, metadata);
            }

            @Override
            public TableMetadata current() {
                return delegate.current();
            }

            @Override
            public TableMetadata refresh() {
                return delegate.refresh();
            }

            @Override
            public FileIO io() {
                return delegate.io();
            }

            @Override
            public EncryptionManager encryption() {
                return delegate.encryption();
            }

            @Override
            public String metadataFileLocation(String fileName) {
                return delegate.metadataFileLocation(fileName);
            }

            @Override
            public LocationProvider locationProvider() {
                return delegate.locationProvider();
            }

            @Override
            public TableOperations temp(TableMetadata uncommittedMetadata) {
                return delegate.temp(uncommittedMetadata);
            }

            @Override
            public long newSnapshotId() {
                return delegate.newSnapshotId();
            }

            @Override
            public boolean requireStrictCleanup() {
                return delegate.requireStrictCleanup();
            }
        };
    }
}
