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

import org.apache.doris.common.Config;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A generic parallel asynchronous processing utility that executes tasks
 * on the system's global shared common thread pool.
 *
 * <p>This class is designed to support high-performance, lightweight,
 * short-lived parallel operations, such as element validation, filtering,
 * data conversion, and status checking. It centralizes thread pool management,
 * timeout control, exception handling, and task cancellation to ensure
 * system stability and unified behavior across all business modules.</p>
 *
 * <p><b>Core Functions:</b>
 * <ul>
 *     <li>{@link #filter(List, Function)}: Parallel asynchronous filtering
 *         that maintains the same input and output data type</li>
 *     <li>{@link #map(List, Function)}: Parallel asynchronous mapping
 *         that transforms input elements into a different output type</li>
 * </ul>
 *
 * <p><b>Usage Constraints & Warnings:</b>
 * <ul>
 *     <li>ONLY for short-running, non-blocking, CPU-bound operations</li>
 *     <li>DO NOT use for I/O-intensive, blocking, or long-running tasks</li>
 *     <li>DO NOT perform thread-blocking operations inside the functions</li>
 *     <li>All tasks respect the global timeout and shared thread pool limits</li>
 * </ul>
 */
public class CommonAsyncProcessor {

    private static final Logger LOG = Logger.getLogger(CommonAsyncProcessor.class);

    /**
     * Global system-shared common business thread pool.
     * <p>This pool is designed for lightweight, fast business operations
     * including filtering, validation, status checks, and data conversion.
     * It is shared across all components to avoid excessive thread creation
     * and resource competition.</p>
     */
    private static final ThreadPoolExecutor COMMON_THREAD_POOL =
            (ThreadPoolExecutor) AsyncThreadPoolFactory.getInstance()
                .getThreadPool(AsyncThreadPoolFactory.ThreadPoolType.COMMON_BUSINESS);

    /**
     * Global unified timeout configuration for all asynchronous tasks.
     * <p>Prevents abnormal tasks from occupying thread resources for an
     * extended period and ensures system fault tolerance.
     * The value is loaded from the global system configuration.</p>
     */
    private static final long GLOBAL_TIMEOUT_MS =
            Config.common_business_async_timeout;

    /**
     * Executes parallel asynchronous filtering on a list of elements.
     *
     * <p>This method processes each element in the provided list using
     * the global common thread pool. The filter function determines whether
     * an element should be retained (return non-null) or discarded (return null).
     * Both input and output types remain identical.</p>
     *
     * <p><b>Typical Usage Scenarios:</b>
     * <ul>
     *     <li>Data validation and filtering</li>
     *     <li>Permission and status pre-checking</li>
     *     <li>Element cleaning and normalization</li>
     * </ul>
     *
     * @param sourceList the original list of elements to be filtered
     * @param filter     asynchronous function that processes one element
     *                   and returns a valid element or null
     * @param <T>        data type of input and output elements
     * @return a non-null list containing only valid filtered elements
     * @throws Exception if task submission fails, execution times out,
     *                   or an internal error occurs during processing
     */
    public static <T> List<T> filter(List<T> sourceList, Function<T, T> filter) throws Exception {
        if (CollectionUtils.isEmpty(sourceList)) {
            return Lists.newArrayList();
        }

        List<CompletableFuture<T>> futures = Lists.newArrayList();

        // 1. Submit asynchronous filter tasks to the shared thread pool
        try {
            for (T item : sourceList) {
                futures.add(CompletableFutureUtil.supplyWithRetry(
                        () -> filter.apply(item),
                        COMMON_THREAD_POOL,
                        GLOBAL_TIMEOUT_MS
                ));
            }
        } catch (Exception e) {
            LOG.warn("Failed to submit asynchronous filter tasks to the common thread pool", e);
            CompletableFutureUtil.cancelAllFutures(futures);
            throw new IllegalStateException("Asynchronous filter task submission failed", e);
        }

        // 2. Wait for all tasks to complete within the global timeout period
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(GLOBAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            LOG.warn("Asynchronous filter tasks timed out in the common thread pool");
            CompletableFutureUtil.cancelAllFutures(futures);
            throw new Exception("Asynchronous filter tasks timed out");
        } catch (Exception e) {
            LOG.warn("Asynchronous filter tasks execution failed", e);
            CompletableFutureUtil.cancelAllFutures(futures);
            throw new IllegalStateException("Asynchronous filter tasks execution failed");
        }

        // 3. Collect valid non-null results from successfully completed tasks
        return futures.stream()
            .filter(CompletableFuture::isDone)
            .map(future -> future.getNow(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /**
     * Executes parallel asynchronous mapping and transformation on a list of elements.
     *
     * <p>This method converts each input element into an object of a different
     * target type using the global common thread pool. It is used for batch
     * data conversion, entity projection, DTO generation, and structured
     * data transformation.</p>
     *
     * <p><b>Typical Usage Scenarios:</b>
     * <ul>
     *     <li>Entity to DTO conversion</li>
     *     <li>Data structure transformation</li>
     *     <li>Batch field extraction and projection</li>
     *     <li>Lightweight data enrichment</li>
     * </ul>
     *
     * @param sourceList the original list of elements to be transformed
     * @param mapper     asynchronous function that converts one element
     *                   from type T to type R
     * @param <T>        input element type
     * @param <R>        output result type
     * @return a non-null list containing successfully transformed elements
     * @throws Exception if task submission fails, execution times out,
     *                   or an internal error occurs during processing
     */
    public static <T, R> List<R> map(List<T> sourceList, Function<T, R> mapper) throws Exception {
        if (CollectionUtils.isEmpty(sourceList)) {
            return Lists.newArrayList();
        }

        List<CompletableFuture<R>> futures = Lists.newArrayList();

        // 1. Submit asynchronous map tasks to the shared thread pool
        try {
            for (T item : sourceList) {
                futures.add(CompletableFutureUtil.supplyWithRetry(
                        () -> mapper.apply(item),
                        COMMON_THREAD_POOL,
                        GLOBAL_TIMEOUT_MS
                ));
            }
        } catch (Exception e) {
            LOG.error("Failed to submit asynchronous map tasks to the common thread pool", e);
            CompletableFutureUtil.cancelAllFutures(futures);
            throw new Exception("Asynchronous map task submission failed");
        }

        // 2. Wait for all tasks to complete within the global timeout period
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(GLOBAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            LOG.error("Asynchronous map tasks timed out in the common thread pool");
            CompletableFutureUtil.cancelAllFutures(futures);
            throw new Exception("Asynchronous map tasks timed out");
        } catch (Exception e) {
            LOG.error("Asynchronous map tasks execution failed", e);
            CompletableFutureUtil.cancelAllFutures(futures);
            throw new Exception("Asynchronous map tasks execution failed");
        }

        // 3. Collect valid non-null results from successfully completed tasks
        return futures.stream()
            .filter(CompletableFuture::isDone)
            .map(future -> future.getNow(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

}
