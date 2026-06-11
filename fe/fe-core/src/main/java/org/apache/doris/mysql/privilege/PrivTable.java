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

package org.apache.doris.mysql.privilege;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.async.AsyncThreadPoolFactory;
import org.apache.doris.common.async.CompletableFutureUtil;
import org.apache.doris.common.io.Text;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public abstract class PrivTable {
    private static final Logger LOG = LogManager.getLogger(PrivTable.class);

    protected Map<PrivKey, PrivEntry> entries;

    // see PrivEntry for more detail
    protected boolean isClassNameWrote = false;

    PrivTable() {
        this.entries = new ConcurrentSkipListMap<>();
    }

    /*
     * Check if user@host has specified privilege
     */
    public boolean hasPriv(PrivPredicate wanted) {
        for (PrivEntry entry : entries.values()) {
            // check priv
            if (entry.privSet.satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Add an entry to priv table.
     * If entry already exists and errOnExist is false, we try to reset or merge the new priv entry with existing one.
     * NOTICE, this method does not set password for the newly added entry if this is a user priv table, the caller
     * need to set password later.
     */
    public PrivEntry addEntry(PrivEntry newEntry,
            boolean errOnExist, boolean errOnNonExist) throws DdlException {
        return addEntry(newEntry, errOnExist, errOnNonExist, false);
    }

    public PrivEntry addEntry(PrivEntry entry, boolean errOnExist, boolean errOnNonExist, boolean isMerge)
            throws DdlException {
        PrivEntry newEntry = entry;
        if (isMerge) {
            try {
                newEntry = entry.copy();
            } catch (AnalysisException | PatternMatcherException e) {
                LOG.error("exception when copy PrivEntry", e);
            }
        }

        PrivEntry existingEntry = getExistingEntry(newEntry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                throw new DdlException("entry does not exist");
            }
            entries.put(newEntry.getPrivKey(), newEntry);
            LOG.info("add priv entry: {} | {}", newEntry.getPrivKey(), newEntry);
            return newEntry;
        } else {
            if (errOnExist) {
                throw new DdlException("entry already exist");
            } else {
                mergePriv(existingEntry, newEntry);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("merge priv entry: {}", existingEntry);
                }
            }
        }
        return existingEntry;
    }

    private void mergePriv(
            PrivEntry first, PrivEntry second) {
        first.getPrivSet().or(second.getPrivSet());
    }

    public List<PrivEntry> getEntries() {
        if (Objects.isNull(entries) || entries.isEmpty()) {
            return Lists.newArrayList();
        }
        return new ArrayList<>(entries.values());
    }

    public void dropEntry(PrivEntry entry) {
        PrivEntry removedEle = entries.remove(entry.getPrivKey());
        LOG.info("drop priv entry: {}", removedEle);
    }

    public void revoke(PrivEntry entry, boolean errOnNonExist,
            boolean deleteEntryWhenEmpty) throws DdlException {
        PrivEntry existingEntry = getExistingEntry(entry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT);
            }
            return;
        }

        // check if privs to be revoked exist in priv entry.
        PrivBitSet tmp = existingEntry.getPrivSet().copy();
        tmp.and(entry.getPrivSet());
        if (tmp.isEmpty()) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT);
            }
            // there is no such priv, nothing need to be done
            return;
        }

        // revoke privs from existing priv entry
        if (LOG.isDebugEnabled()) {
            LOG.debug("before revoke: {}, privs to be revoked: {}",
                    existingEntry.getPrivSet(), entry.getPrivSet());
        }
        tmp = existingEntry.getPrivSet().copy();
        tmp.xor(entry.getPrivSet());
        existingEntry.getPrivSet().and(tmp);
        if (LOG.isDebugEnabled()) {
            LOG.debug("after revoke: {}", existingEntry);
        }

        if (existingEntry.getPrivSet().isEmpty() && deleteEntryWhenEmpty) {
            // no priv exists in this entry, remove it
            dropEntry(existingEntry);
        }
    }

    // Get existing entry which is the keys match the given entry
    protected PrivEntry getExistingEntry(PrivEntry entry) {
        return  entries.get(entry.getPrivKey());
    }

    /**
     * Performs short-circuit privilege rule matching with adaptive execution strategy.
     * Automatically switches between synchronous and parallel modes based on entry list size.
     * <p>
     * Core design principles:
     * 1. Adaptive execution: Synchronous mode for small datasets (zero overhead),
     *    parallel mode for large datasets (multi-core utilization)
     * 2. Batch processing: Merges multiple entries into single tasks to minimize
     *    thread scheduling and context switching overhead
     * 3. Configurable batch size: Allows precise control over task execution duration
     *    to balance parallelism and overhead
     * 4. Global short-circuit: Returns immediately when first valid match is found
     * 5. Guaranteed resource cleanup: Cancels all remaining tasks upon any exit
     * 6. Robust failure handling: Includes exponential backoff retry for task submission
     *
     * @param <T>    Type of input entry elements to be matched
     * @param <R>    Type of non-null match result returned by the matching function
     * @param entries List of entries to perform matching against
     * @param func    Function that executes matching logic for each entry
     * @return First non-null matching result, or null if no valid match is found
     * @throws IllegalStateException If any error, timeout, or execution failure occurs
     */
    protected <T, R> R doPrivMatch(List<T> entries, Function<T, R> func) {
        if (CollectionUtils.isEmpty(entries)) {
            return null;
        }

        if (entries.size() < Config.parallel_auth_check_threshold) {
            // Synchronous mode: Optimal for small datasets, eliminates parallel overhead
            return doBatch(entries, func);
        } else {
            // Parallel mode: Leverages multi-core processing for large datasets
            ThreadPoolExecutor executor = (ThreadPoolExecutor) AsyncThreadPoolFactory.getInstance()
                    .getThreadPool(AsyncThreadPoolFactory.ThreadPoolType.PRIVILEGE_CHECK);

            int totalPolicies = entries.size();
            int batchSize = Config.parallel_auth_batch_size;
            int totalTasks = (totalPolicies + batchSize - 1) / batchSize;

            List<CompletableFuture<R>> allFutures = new ArrayList<>(totalTasks);

            // Submit all tasks with exponential backoff retry
            for (int i = 0; i < totalTasks; i++) {
                int start = i * batchSize;
                int end = Math.min(start + batchSize, totalPolicies);

                if (start >= end) {
                    break;
                }

                List<T> batchEntries = entries.subList(start, end);
                try {
                    CompletableFuture<R> future = CompletableFutureUtil.supplyWithRetry(
                            () -> doBatch(batchEntries, func), executor, Config.parallel_auth_check_timeout);
                    allFutures.add(future);
                } catch (Exception e) {
                    CompletableFutureUtil.cancelAllFutures(allFutures);
                    throw e;
                }
            }

            // Global result aggregator with short-circuit evaluation
            CompletableFuture<R> firstMatch = new CompletableFuture<>();
            AtomicInteger completedNullCount = new AtomicInteger(0);

            allFutures.forEach(future -> {
                future.thenAccept(res -> {
                    if (res != null) {
                        firstMatch.complete(res);
                        return;
                    }
                    if (completedNullCount.incrementAndGet() == allFutures.size()) {
                        firstMatch.complete(null);
                    }
                }).exceptionally(e -> {
                    if (!firstMatch.isDone()) {
                        firstMatch.completeExceptionally(e);
                    }
                    return null;
                });
            });

            try {
                // Wait for first result with global timeout
                return firstMatch.get(Config.parallel_auth_check_timeout, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("Privilege check failed", e);
            } finally {
                // Cancel remaining tasks for normal return and waiting phase exceptions
                CompletableFutureUtil.cancelAllFutures(allFutures);
            }
        }
    }



    /**
     * Processes a batch of entries sequentially with short-circuit evaluation.
     * Shared execution logic for both synchronous and parallel modes.
     *
     * @param batch Batch of entries to process
     * @param func  Matching function to apply to each entry
     * @param <T>   Type of input entry elements
     * @param <R>   Type of match result
     * @return First non-null match result in the batch, or null if no match found
     */
    private <T, R> R doBatch(List<T> batch, Function<T, R> func) {
        if (CollectionUtils.isEmpty(batch)) {
            return null;
        }
        return batch.stream()
            .map(func)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }


    // for test only
    public void clear() {
        entries.clear();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Deprecated
    public static PrivTable read(DataInput in) throws IOException {
        String className = Text.readString(in);
        PrivTable privTable;
        try {
            Class<? extends PrivTable> derivedClass = (Class<? extends PrivTable>) Class.forName(className);
            privTable = derivedClass.newInstance();
            Class[] paramTypes = {DataInput.class};
            Method readMethod = derivedClass.getMethod("readFields", paramTypes);
            Object[] params = {in};
            readMethod.invoke(privTable, params);

            return privTable;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new IOException("failed read PrivTable", e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n");
        for (Map.Entry<PrivKey, PrivEntry> entry : entries.entrySet()) {
            PrivKey privKey = entry.getKey();
            PrivEntry privEntry = entry.getValue();
            sb.append(privKey).append(" | ").append(privEntry).append("\n");
        }
        return sb.toString();
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            PrivEntry entry = PrivEntry.read(in);
            entries.put(entry.getPrivKey(), entry);
        }
    }

    public void merge(PrivTable privTable) {
        for (PrivEntry entry : privTable.getEntries()) {
            try {
                addEntry(entry, false, false, true);
            } catch (DdlException e) {
                //will no exception
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e.getMessage());
                }
            }
        }
    }
}
