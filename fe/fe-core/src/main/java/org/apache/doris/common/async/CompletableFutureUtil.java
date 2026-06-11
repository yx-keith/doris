// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.async;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public class CompletableFutureUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CompletableFutureUtil.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private CompletableFutureUtil() {}

    /**
     * Submits an asynchronous task with exponential backoff retry mechanism.
     *
     * <p>Design ideas:
     * 1. When the thread pool is full (RejectedExecutionException), do not fail immediately.
     * 2. Use exponential backoff (100ms → 200ms → 400ms → ...) to retry submission.
     * 3. Enforce global timeout to avoid infinite blocking.
     * 4. Improve system stability under high concurrency.
     *
     * <p>Retry strategy:
     * - Initial backoff: 100ms
     * - Exponential growth: multiply by 2 each time
     * - Maximum backoff: timeout / 2
     * - Total execution time cannot exceed the given timeout
     *
     * @param supplier Task logic to execute asynchronously
     * @param executor Thread pool to run the task
     * @param timeout  Maximum time (ms) for task submission and retry
     * @return A CompletableFuture that holds the task result
     * @throws IllegalStateException If task submission times out after multiple retries
     * @throws RejectedExecutionException If thread pool is exhausted and retry fails
     */
    public static <U> CompletableFuture<U> supplyWithRetry(@NotNull Supplier<U> supplier, @NotNull Executor executor,
                                                           long timeout) {
        long startTime = System.currentTimeMillis();
        int attempt = 0;
        long currentBackoff = 100;

        while (true) {
            long elapse = System.currentTimeMillis() - startTime;
            if (elapse > timeout) {
                throw new IllegalStateException(String.format("Async task submission timed out. Total attempts: %d, "
                    + "elapsed time(ms): %d", attempt, elapse));
            }

            try {
                attempt++;
                return CompletableFuture.supplyAsync(supplier, executor);
            } catch (RejectedExecutionException e) {
                long remainingWaitMs = timeout - (System.currentTimeMillis() - startTime);
                long actualWait = Math.min(currentBackoff, remainingWaitMs);
                if (attempt % 3 == 0) {
                    LOG.warn("Async task retry attempt {} after {}ms backoff.", attempt + 1, actualWait);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(actualWait));
                currentBackoff = Math.min(currentBackoff * 2, timeout / 2);
            }
        }
    }


    /**
     * Cancels all incomplete futures in the given list.
     * Unified resource cleanup method used by both submission and waiting phases.
     *
     * @param futures List of futures to cancel
     * @param <U>     Type of future result
     */
    public static <U> void cancelAllFutures(List<CompletableFuture<U>> futures) {
        if (Objects.isNull(futures) || futures.isEmpty()) {
            return;
        }
        for (CompletableFuture<U> future : futures) {
            if (Objects.nonNull(future) && !future.isDone()) {
                future.cancel(false);
            }
        }
    }

}
