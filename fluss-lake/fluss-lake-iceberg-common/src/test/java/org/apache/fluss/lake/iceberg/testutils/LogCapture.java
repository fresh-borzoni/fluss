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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Captures the INFO messages of a logger and its children, e.g. of the tiering committer running in
 * the test's Flink mini cluster, while the test logs nothing otherwise.
 */
public final class LogCapture implements AutoCloseable {

    private final String loggerName;
    private final Appender appender;

    private LogCapture(String loggerName) {
        this.loggerName = loggerName;
        this.appender = new Appender("capture-" + loggerName);
        appender.start();
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();
        LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.INFO, false);
        loggerConfig.addAppender(appender, Level.INFO, null);
        config.addLogger(loggerName, loggerConfig);
        context.updateLoggers();
    }

    /** Captures the INFO messages of {@code loggerName} and its children until closed. */
    public static LogCapture of(String loggerName) {
        return new LogCapture(loggerName);
    }

    /** The captured messages that contain all of {@code parts}. */
    public List<String> messagesWith(String... parts) {
        return appender.messages.stream()
                .filter(
                        m -> {
                            for (String part : parts) {
                                if (!m.contains(part)) {
                                    return false;
                                }
                            }
                            return true;
                        })
                .collect(Collectors.toList());
    }

    public List<String> messages() {
        return new ArrayList<>(appender.messages);
    }

    /**
     * Blocks the thread that logs the next message containing all of {@code parts} until the pause
     * is closed or the thread is interrupted, e.g. a tiering job's operator at a point the test
     * then cancels the job at.
     */
    public Pause pauseAt(String... parts) {
        Pause pause = new Pause(parts);
        appender.pause = pause;
        return pause;
    }

    @Override
    public void close() {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.getConfiguration().removeLogger(loggerName);
        context.updateLoggers();
        appender.stop();
    }

    /** A pause of the thread that logs a message, see {@link #pauseAt}. */
    public static final class Pause implements AutoCloseable {

        private final String[] parts;
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        private Pause(String[] parts) {
            this.parts = parts;
        }

        /** Waits until a thread logged the message and is paused. */
        public void awaitReached(Duration timeout) throws InterruptedException {
            if (!reached.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new AssertionError(
                        "No message with " + Arrays.toString(parts) + " within " + timeout);
            }
        }

        /** Releases the paused thread, if any; a later message does not pause. */
        @Override
        public void close() {
            reached.countDown();
            released.countDown();
        }

        private boolean matches(String message) {
            for (String part : parts) {
                if (!message.contains(part)) {
                    return false;
                }
            }
            return true;
        }

        private void pause() {
            if (reached.getCount() == 0) {
                return;
            }
            reached.countDown();
            try {
                released.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class Appender extends AbstractAppender {

        private final List<String> messages = new CopyOnWriteArrayList<>();
        private volatile Pause pause;

        private Appender(String name) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            messages.add(message);
            Pause current = pause;
            if (current != null && current.matches(message)) {
                current.pause();
            }
        }
    }
}
