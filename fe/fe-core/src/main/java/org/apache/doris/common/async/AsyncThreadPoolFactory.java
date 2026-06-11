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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Unified factory for managing all asynchronous thread pools in the system.
 * <p>
 * Features:
 * 1. Global singleton instance for each thread pool type
 * 2. Lazy initialization - created only on first access
 * 3. Thread-safe concurrent access and initialization
 * 4. Centralized configuration management with per-pool customization
 * 5. Graceful shutdown mechanism with JVM shutdown hook
 * 6. Built-in monitoring capabilities for observability
 */

public final class AsyncThreadPoolFactory {
    private static final Logger log = LoggerFactory.getLogger(AsyncThreadPoolFactory.class);

    // Singleton instance with double-checked locking
    private static volatile AsyncThreadPoolFactory instance;

    /**
     * Thread pool cache: key = pool type, value = executor service instance
     * Uses ConcurrentHashMap for thread-safe concurrent access
     */
    private final Map<ThreadPoolType, ExecutorService> threadPoolCache = new ConcurrentHashMap<>();

    /**
     * Configuration provider for thread pool parameters
     */
    private final ThreadPoolConfigProvider configProvider;

    /**
     * Shutdown flag to prevent pool creation after factory shutdown
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Private constructor to enforce singleton pattern
     *
     * @param configProvider provider for thread pool configurations
     */
    private AsyncThreadPoolFactory(ThreadPoolConfigProvider configProvider) {
        this.configProvider = Objects.requireNonNull(configProvider, "Config provider cannot be null");
        // Register JVM shutdown hook for graceful resource release
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownAll));
    }

    /**
     * Get the singleton instance of the thread pool factory
     *
     * @param configProvider configuration provider for thread pools
     * @return singleton factory instance
     */
    public static AsyncThreadPoolFactory getInstance(ThreadPoolConfigProvider configProvider) {
        if (instance == null) {
            synchronized (AsyncThreadPoolFactory.class) {
                if (instance == null) {
                    if (configProvider == null) {
                        configProvider = new DefaultThreadPoolConfigProvider();
                    }
                    instance = new AsyncThreadPoolFactory(configProvider);
                }
            }
        }
        return instance;
    }

    /**
     * Get the singleton instance of the thread pool factory use default config provider
     *
     * @return singleton factory instance
     */
    public static AsyncThreadPoolFactory getInstance() {
        return getInstance(null);
    }

    /**
     * Get thread pool instance by type (lazy initialization, thread-safe)
     *
     * @param type type of thread pool to retrieve
     * @return singleton executor service instance
     * @throws IllegalStateException if factory has been shutdown
     */
    public ExecutorService getThreadPool(ThreadPoolType type) {
        Objects.requireNonNull(type, "ThreadPoolType cannot be null");

        if (isShutdown.get()) {
            throw new IllegalStateException("AsyncThreadPoolFactory has been shutdown");
        }

        // Atomic operation guarantees single initialization per type
        return threadPoolCache.computeIfAbsent(type, this::createThreadPool);
    }

    /**
     * Create a new thread pool instance with the specified configuration
     *
     * @param type type of thread pool to create
     * @return newly created executor service
     */
    private ExecutorService createThreadPool(ThreadPoolType type) {
        ThreadPoolConfig config = configProvider.getConfig(type);
        log.info("Initializing thread pool: {}, configuration: {}", type, config);

        // Custom thread factory with meaningful naming and priority
        ThreadFactory threadFactory = new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
            private final AtomicInteger threadCounter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = defaultFactory.newThread(r);
                thread.setName(String.format("%s-pool-%d", type.getPoolName(), threadCounter.incrementAndGet()));
                thread.setDaemon(false); // Non-daemon to ensure task completion
                thread.setPriority(config.getThreadPriority());
                return thread;
            }
        };

        return new ThreadPoolExecutor(
            config.getCorePoolSize(),
            config.getMaximumPoolSize(),
            config.getKeepAliveTime(),
            config.getTimeUnit(),
            config.getWorkQueue(),
            threadFactory,
            config.getRejectedExecutionHandler()
        );
    }

    /**
     * Gracefully shutdown all thread pools.
     * First attempts normal shutdown, waits for tasks to complete,
     * then forces shutdown if timeout is exceeded.
     */
    public void shutdownAll() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.warn("AsyncThreadPoolFactory has already been shutdown");
            return;
        }

        log.info("Initiating shutdown of all thread pools. Total pools: {}", threadPoolCache.size());

        // Step 1: Stop accepting new tasks
        threadPoolCache.values().forEach(ExecutorService::shutdown);

        // Step 2: Wait for existing tasks to complete
        try {
            for (Map.Entry<ThreadPoolType, ExecutorService> entry : threadPoolCache.entrySet()) {
                ThreadPoolType type = entry.getKey();
                ExecutorService pool = entry.getValue();

                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("Thread pool {} did not terminate within 30 seconds, forcing shutdown", type);
                    pool.shutdownNow(); // Interrupt running tasks

                    if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.error("Thread pool {} failed to terminate even after force shutdown", type);
                    }
                } else {
                    log.info("Thread pool {} shutdown completed successfully", type);
                }
            }
        } catch (InterruptedException e) {
            log.error("Thread pool shutdown process interrupted", e);
            Thread.currentThread().interrupt();
            // Force shutdown all pools on interrupt
            threadPoolCache.values().forEach(ExecutorService::shutdownNow);
        }

        threadPoolCache.clear();
        log.info("All thread pools have been successfully shutdown");
    }

    /**
     * Get monitoring information for a specific thread pool
     *
     * @param type type of thread pool to monitor
     * @return monitoring metrics DTO, null if pool does not exist
     */
    public ThreadPoolMonitorInfo getMonitorInfo(ThreadPoolType type) {
        ExecutorService service = threadPoolCache.get(type);
        if (!(service instanceof ThreadPoolExecutor)) {
            return null;
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) service;
        return new ThreadPoolMonitorInfo(
            type.getPoolName(),
            executor.getCorePoolSize(),
            executor.getMaximumPoolSize(),
            executor.getPoolSize(),
            executor.getActiveCount(),
            executor.getQueue().size(),
            executor.getCompletedTaskCount(),
            executor.getTaskCount()
        );
    }

    /**
     * Enumeration of all thread pool types in the system.
     * Add new types here when additional thread pools are needed.
     */
    public enum ThreadPoolType {
        /**
         * Dedicated thread pool for privilege check operations
         */
        PRIVILEGE_CHECK("privilege-check"),

        /**
         * General purpose thread pool for common business tasks
         */
        COMMON_BUSINESS("common-business");


        private final String poolName;

        ThreadPoolType(String poolName) {
            this.poolName = poolName;
        }

        public String getPoolName() {
            return poolName;
        }
    }

    /**
     * Configuration DTO for thread pool parameters
     */
    public static class ThreadPoolConfig {
        private int corePoolSize;
        private int maximumPoolSize;
        private long keepAliveTime;
        private TimeUnit timeUnit;
        private BlockingQueue<Runnable> workQueue;
        private RejectedExecutionHandler rejectedExecutionHandler;
        private int threadPriority = Thread.NORM_PRIORITY;
        private int totalQueueCapacity;

        /**
         * Full constructor with all parameters
         */
        public ThreadPoolConfig(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                TimeUnit timeUnit, BlockingQueue<Runnable> workQueue,
                                RejectedExecutionHandler rejectedExecutionHandler) {
            this.corePoolSize = corePoolSize;
            this.maximumPoolSize = maximumPoolSize;
            this.keepAliveTime = keepAliveTime;
            this.timeUnit = timeUnit;
            this.workQueue = workQueue;
            this.rejectedExecutionHandler = rejectedExecutionHandler;
            this.totalQueueCapacity = workQueue.remainingCapacity() + workQueue.size();
        }

        /**
         * Convenience constructor with default rejection policy and thread priority
         */
        public ThreadPoolConfig(int corePoolSize, int maximumPoolSize, long keepAliveTime,
                                TimeUnit timeUnit, BlockingQueue<Runnable> workQueue) {
            this(corePoolSize, maximumPoolSize, keepAliveTime, timeUnit, workQueue,
                new ThreadPoolExecutor.AbortPolicy());
        }

        // Getters and Setters
        public int getCorePoolSize() {
            return corePoolSize;
        }

        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }

        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        public void setMaximumPoolSize(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
        }

        public long getKeepAliveTime() {
            return keepAliveTime;
        }

        public void setKeepAliveTime(long keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public void setTimeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        public BlockingQueue<Runnable> getWorkQueue() {
            return workQueue;
        }

        public void setWorkQueue(BlockingQueue<Runnable> workQueue) {
            this.workQueue = workQueue;
            this.totalQueueCapacity = workQueue.remainingCapacity() + workQueue.size();
        }

        public RejectedExecutionHandler getRejectedExecutionHandler() {
            return rejectedExecutionHandler;
        }

        public void setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
            this.rejectedExecutionHandler = rejectedExecutionHandler;
        }

        public int getThreadPriority() {
            return threadPriority;
        }

        public void setThreadPriority(int threadPriority) {
            this.threadPriority = threadPriority;
        }

        @Override
        public String toString() {
            return new StringBuilder().append("ThreadPoolConfig{")
                .append("corePoolSize=").append(corePoolSize)
                .append(", maximumPoolSize=").append(maximumPoolSize)
                .append(", keepAliveTime=").append(keepAliveTime)
                .append(", timeUnit=").append(timeUnit)
                .append(", queueType=").append(workQueue.getClass().getSimpleName())
                .append(", remainingCapacity=")
                .append(workQueue.remainingCapacity() == Integer.MAX_VALUE ? "unbounded" :
                    workQueue.remainingCapacity())
                .append(", queueCapacity=")
                .append(totalQueueCapacity == Integer.MAX_VALUE ? "unbounded" : totalQueueCapacity)
                .append(", rejectedHandler=").append(rejectedExecutionHandler.getClass().getSimpleName())
                .append('}').toString();
        }
    }

    /**
     * Provider interface for thread pool configurations.
     * Implementations can load configurations from any source (properties, database, etc.)
     */
    public interface ThreadPoolConfigProvider {
        ThreadPoolConfig getConfig(ThreadPoolType type);
    }

    /**
     * DTO containing thread pool monitoring metrics
     */
    public static class ThreadPoolMonitorInfo {
        private final String poolName;
        private final int corePoolSize;
        private final int maximumPoolSize;
        private final int currentPoolSize;
        private final int activeThreadCount;
        private final int queueSize;
        private final long completedTaskCount;
        private final long totalTaskCount;

        public ThreadPoolMonitorInfo(String poolName, int corePoolSize, int maximumPoolSize,
                                     int currentPoolSize, int activeThreadCount, int queueSize,
                                     long completedTaskCount, long totalTaskCount) {
            this.poolName = poolName;
            this.corePoolSize = corePoolSize;
            this.maximumPoolSize = maximumPoolSize;
            this.currentPoolSize = currentPoolSize;
            this.activeThreadCount = activeThreadCount;
            this.queueSize = queueSize;
            this.completedTaskCount = completedTaskCount;
            this.totalTaskCount = totalTaskCount;
        }

        // Getters
        public String getPoolName() {
            return poolName;
        }

        public int getCorePoolSize() {
            return corePoolSize;
        }

        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        public int getCurrentPoolSize() {
            return currentPoolSize;
        }

        public int getActiveThreadCount() {
            return activeThreadCount;
        }

        public int getQueueSize() {
            return queueSize;
        }

        public long getCompletedTaskCount() {
            return completedTaskCount;
        }

        public long getTotalTaskCount() {
            return totalTaskCount;
        }

        @Override
        public String toString() {
            return new StringBuilder().append("ThreadPoolMonitorInfo{")
                .append("poolName='").append(poolName)
                .append('\'').append(", corePoolSize=").append(corePoolSize)
                .append(", maximumPoolSize=").append(maximumPoolSize)
                .append(", currentPoolSize=").append(currentPoolSize)
                .append(", activeThreadCount=").append(activeThreadCount)
                .append(", queueSize=").append(queueSize)
                .append(", completedTaskCount=").append(completedTaskCount)
                .append(", totalTaskCount=").append(totalTaskCount)
                .append('}').toString();
        }
    }
}
