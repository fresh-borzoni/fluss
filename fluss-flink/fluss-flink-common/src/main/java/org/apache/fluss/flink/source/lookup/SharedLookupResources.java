/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.epochs.EpochRegistry;
import org.apache.fluss.flink.source.epochs.EpochResolver;
import org.apache.fluss.flink.source.epochs.EpochSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Connections and epoch resolvers shared by the lookup functions of one JVM, closed with their last
 * user. A join opens a lookup function per table, direction and subtask, which would otherwise each
 * hold a client and resolve the same epochs again.
 */
final class SharedLookupResources {

    private static final Logger LOG = LoggerFactory.getLogger(SharedLookupResources.class);

    private static final Map<Map<String, String>, Counted<Connection>> CONNECTIONS =
            new HashMap<>();
    private static final Map<List<Object>, Counted<Epochs>> EPOCHS = new HashMap<>();

    private SharedLookupResources() {}

    static Connection acquireConnection(Configuration config) {
        synchronized (CONNECTIONS) {
            Counted<Connection> counted =
                    CONNECTIONS.computeIfAbsent(
                            config.toMap(),
                            key -> {
                                LOG.info("Opening a shared lookup connection.");
                                return new Counted<>(ConnectionFactory.createConnection(config));
                            });
            counted.users++;
            return counted.value;
        }
    }

    static void releaseConnection(Configuration config, Connection connection) throws Exception {
        Connection toClose = null;
        synchronized (CONNECTIONS) {
            Counted<Connection> counted = CONNECTIONS.get(config.toMap());
            if (counted != null && --counted.users == 0) {
                CONNECTIONS.remove(config.toMap());
                toClose = counted.value;
            }
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    /** The registry, retry thread and resolver of the epochs of one database and domain. */
    static final class Epochs {
        final EpochRegistry registry;
        final ScheduledExecutorService retries;
        final EpochResolver resolver;

        Epochs(EpochRegistry registry, ScheduledExecutorService retries, EpochResolver resolver) {
            this.registry = registry;
            this.retries = retries;
            this.resolver = resolver;
        }

        void close() throws Exception {
            retries.shutdownNow();
            registry.close();
        }
    }

    static Epochs acquireEpochs(
            Configuration config, Connection connection, String database, EpochSettings epochs)
            throws Exception {
        List<Object> key = epochsKey(config, database, epochs);
        synchronized (EPOCHS) {
            Counted<Epochs> counted = EPOCHS.get(key);
            if (counted == null) {
                counted = new Counted<>(openEpochs(connection, database, epochs));
                EPOCHS.put(key, counted);
            }
            counted.users++;
            return counted.value;
        }
    }

    static void releaseEpochs(
            Configuration config, String database, EpochSettings epochs, Epochs shared)
            throws Exception {
        List<Object> key = epochsKey(config, database, epochs);
        Epochs toClose = null;
        synchronized (EPOCHS) {
            Counted<Epochs> counted = EPOCHS.get(key);
            if (counted != null && --counted.users == 0) {
                EPOCHS.remove(key);
                toClose = counted.value;
            }
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    private static List<Object> epochsKey(
            Configuration config, String database, EpochSettings epochs) {
        return Arrays.asList(config.toMap(), database, epochs.getDomain(), epochs.getIntervalMs());
    }

    private static Epochs openEpochs(Connection connection, String database, EpochSettings epochs)
            throws Exception {
        EpochRegistry registry =
                EpochRegistry.open(connection, false, database, epochs.getDomain());
        ScheduledExecutorService retries =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r, "fluss-lookup-epochs-" + database);
                            thread.setDaemon(true);
                            return thread;
                        });
        // a scan ends an epoch once per interval: look a few times per interval
        EpochResolver resolver =
                new EpochResolver(
                        registry, retries, Math.max(1L, Math.min(20L, epochs.getIntervalMs() / 4)));
        return new Epochs(registry, retries, resolver);
    }

    private static final class Counted<T> {
        final T value;
        int users;

        Counted(T value) {
            this.value = value;
        }
    }
}
