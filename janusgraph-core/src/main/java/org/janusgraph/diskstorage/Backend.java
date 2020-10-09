// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import org.janusgraph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.ExpirationKCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.NoKCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanner;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.LockerProvider;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStoreManager;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.LogManager;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.MetricInstrumentedStoreManager;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Orchestrates and configures all backend systems:
 * The primary backend storage ({@link KeyColumnValueStore}) and all external indexing providers ({@link IndexProvider}).
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Backend implements LockerProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Backend.class);

    /**
     * These are the names for the edge store and property index databases, respectively.
     * The edge store contains all edges and properties. The property index contains an
     * inverted index from attribute value to vertex.
     * <p>
     * These names are fixed and should NEVER be changed. Changing these strings can
     * disrupt storage adapters that rely on these names for specific configurations.
     * In the past, the store name for the ID table, janusgraph_ids, was also marked here,
     * but to clear the upgrade path from Titan to JanusGraph, we had to pull it into
     * configuration.
     */
    public static final String EDGESTORE_NAME = "edgestore";
    public static final String INDEXSTORE_NAME = "graphindex";

    public static final String METRICS_STOREMANAGER_NAME = "storeManager";
    public static final String METRICS_MERGED_STORE = "stores";
    public static final String METRICS_MERGED_CACHE = "caches";
    public static final String METRICS_CACHE_SUFFIX = ".cache";
    public static final String LOCK_STORE_SUFFIX = "_lock_";

    public static final String SYSTEM_TX_LOG_NAME = "txlog";
    public static final String SYSTEM_MGMT_LOG_NAME = "systemlog";

    public static final double EDGESTORE_CACHE_PERCENT = 0.8;
    public static final double INDEXSTORE_CACHE_PERCENT = 0.2;

    private static final long ETERNAL_CACHE_EXPIRATION = 1000L *3600*24*365*200; //200 years

    public static final int THREAD_POOL_SIZE_SCALE_FACTOR = 2;

    //############ Registered Storage Managers ##############

    private static final Map<StandardStoreManager, ConfigOption<?>> STORE_SHORTHAND_OPTIONS = Collections.unmodifiableMap(new HashMap<StandardStoreManager, ConfigOption<?>>() {{
        put(StandardStoreManager.BDB_JE, STORAGE_DIRECTORY);
        put(StandardStoreManager.CQL, STORAGE_HOSTS);
        put(StandardStoreManager.HBASE, STORAGE_HOSTS);
        //put(StandardStorageBackend.IN_MEMORY, null);
    }});

    public static final Map<String,String> REGISTERED_LOG_MANAGERS = new HashMap<String, String>() {{
        put("default","org.janusgraph.diskstorage.log.kcvs.KCVSLogManager");
    }};

    private static final Function<String, Locker> TEST_LOCKER_CREATOR = lockerName -> openManagedLocker("org.janusgraph.diskstorage.util.TestLockerManager",lockerName);

    private final Function<String, Locker> CONSISTENT_KEY_LOCKER_CREATOR = new Function<String, Locker>() {
        @Override
        public Locker apply(String lockerName) {
            KeyColumnValueStore lockerStore;
            try {
                lockerStore = storeManager.openDatabase(lockerName);
            } catch (BackendException e) {
                throw new JanusGraphConfigurationException("Could not retrieve store named " + lockerName + " for locker configuration", e);
            }
            return new ConsistentKeyLocker.Builder(lockerStore, storeManager).fromConfig(configuration).build();
        }
    };

    private final Map<String, Function<String, Locker>> REGISTERED_LOCKERS = Collections.unmodifiableMap(
        new HashMap<String, Function<String, Locker>>(2){{
            put("consistentkey", CONSISTENT_KEY_LOCKER_CREATOR);
            put("test", TEST_LOCKER_CREATOR);
        }});

    private final KeyColumnValueStoreManager storeManager;
    private final KeyColumnValueStoreManager storeManagerLocking;
    private final StoreFeatures storeFeatures;

    private KCVSCache edgeStore;
    private KCVSCache indexStore;
    private KCVSCache txLogStore;
    private IDAuthority idAuthority;
    private KCVSConfiguration systemConfig;
    private KCVSConfiguration userConfig;
    private boolean hasAttemptedClose;

    private final StandardScanner scanner;

    private final KCVSLogManager managementLogManager;
    private final KCVSLogManager txLogManager;
    private final LogManager userLogManager;


    private final Map<String, IndexProvider> indexes;

    private final int bufferSize;
    private final Duration maxWriteTime;
    private final Duration maxReadTime;
    private final boolean cacheEnabled;
    private final ExecutorService threadPool;

    private final Function<String, Locker> lockerCreator;
    private final ConcurrentHashMap<String, Locker> lockers = new ConcurrentHashMap<>();

    private final Configuration configuration;

    public Backend(Configuration configuration) {
        this.configuration = configuration;

        KeyColumnValueStoreManager manager = getStorageManager(configuration);
        if (configuration.get(BASIC_METRICS)) {
            storeManager = new MetricInstrumentedStoreManager(manager,METRICS_STOREMANAGER_NAME,configuration.get(METRICS_MERGE_STORES),METRICS_MERGED_STORE);
        } else {
            storeManager = manager;
        }
        indexes = getIndexes(configuration);
        storeFeatures = storeManager.getFeatures();

        managementLogManager = getKCVSLogManager(MANAGEMENT_LOG);
        txLogManager = getKCVSLogManager(TRANSACTION_LOG);
        userLogManager = getLogManager(USER_LOG);


        cacheEnabled = !configuration.get(STORAGE_BATCH) && configuration.get(DB_CACHE);

        int bufferSizeTmp = configuration.get(BUFFER_SIZE);
        Preconditions.checkArgument(bufferSizeTmp > 0, "Buffer size must be positive");
        if (!storeFeatures.hasBatchMutation()) {
            bufferSize = Integer.MAX_VALUE;
        } else bufferSize = bufferSizeTmp;

        maxWriteTime = configuration.get(STORAGE_WRITE_WAITTIME);
        maxReadTime = configuration.get(STORAGE_READ_WAITTIME);

        if (!storeFeatures.hasLocking()) {
            Preconditions.checkArgument(storeFeatures.isKeyConsistent(),"Store needs to support some form of locking");
            storeManagerLocking = new ExpectedValueCheckingStoreManager(storeManager,LOCK_STORE_SUFFIX,this,maxReadTime);
        } else {
            storeManagerLocking = storeManager;
        }

        if (configuration.get(PARALLEL_BACKEND_OPS)) {
            int poolSize = Runtime.getRuntime().availableProcessors() * THREAD_POOL_SIZE_SCALE_FACTOR;
            threadPool = Executors.newFixedThreadPool(poolSize);
            log.info("Initiated backend operations thread pool of size {}", poolSize);
        } else {
            threadPool = null;
        }

        final String lockBackendName = configuration.get(LOCK_BACKEND);
        if (REGISTERED_LOCKERS.containsKey(lockBackendName)) {
            lockerCreator = REGISTERED_LOCKERS.get(lockBackendName);
        } else {
            throw new JanusGraphConfigurationException("Unknown lock backend \"" +
                    lockBackendName + "\".  Known lock backends: " +
                    String.join(", ", REGISTERED_LOCKERS.keySet()) + ".");
        }
        // Never used for backends that have innate transaction support, but we
        // want to maintain the non-null invariant regardless; it will default
        // to consistent-key implementation if none is specified
        Preconditions.checkNotNull(lockerCreator);

        scanner = new StandardScanner(storeManager);
    }


    @Override
    public Locker getLocker(String lockerName) {

        Preconditions.checkNotNull(lockerName);

        Locker l = lockers.get(lockerName);

        if (null == l) {
            l = lockerCreator.apply(lockerName);
            final Locker x = lockers.putIfAbsent(lockerName, l);
            if (null != x) {
                l = x;
            }
        }

        return l;
    }


    /**
     * Initializes this backend with the given configuration. Must be called before this Backend can be used
     *
     * @param config
     */
    public void initialize(Configuration config) {
        try {
            //EdgeStore & VertexIndexStore
            KeyColumnValueStore idStore = storeManager.openDatabase(config.get(IDS_STORE_NAME));

            idAuthority = null;
            if (storeFeatures.isKeyConsistent()) {
                idAuthority = new ConsistentKeyIDAuthority(idStore, storeManager, config);
            } else {
                throw new IllegalStateException("Store needs to support consistent key or transactional operations for ID manager to guarantee proper id allocations");
            }

            KeyColumnValueStore edgeStoreRaw = storeManagerLocking.openDatabase(EDGESTORE_NAME);
            KeyColumnValueStore indexStoreRaw = storeManagerLocking.openDatabase(INDEXSTORE_NAME);

            //Configure caches
            if (cacheEnabled) {
                long expirationTime = configuration.get(DB_CACHE_TIME);
                Preconditions.checkArgument(expirationTime>=0,"Invalid cache expiration time: %s",expirationTime);
                if (expirationTime==0) expirationTime=ETERNAL_CACHE_EXPIRATION;

                long cacheSizeBytes;
                double cacheSize = configuration.get(DB_CACHE_SIZE);
                Preconditions.checkArgument(cacheSize>0.0,"Invalid cache size specified: %s",cacheSize);
                if (cacheSize<1.0) {
                    //Its a percentage
                    Runtime runtime = Runtime.getRuntime();
                    cacheSizeBytes = (long)((runtime.maxMemory()-(runtime.totalMemory()-runtime.freeMemory())) * cacheSize);
                } else {
                    Preconditions.checkArgument(cacheSize>1000,"Cache size is too small: %s",cacheSize);
                    cacheSizeBytes = (long)cacheSize;
                }
                log.info("Configuring total store cache size: {}",cacheSizeBytes);
                long cleanWaitTime = configuration.get(DB_CACHE_CLEAN_WAIT);
                Preconditions.checkArgument(EDGESTORE_CACHE_PERCENT + INDEXSTORE_CACHE_PERCENT == 1.0,"Cache percentages don't add up!");
                long edgeStoreCacheSize = Math.round(cacheSizeBytes * EDGESTORE_CACHE_PERCENT);
                long indexStoreCacheSize = Math.round(cacheSizeBytes * INDEXSTORE_CACHE_PERCENT);

                edgeStore = new ExpirationKCVSCache(edgeStoreRaw,getMetricsCacheName(EDGESTORE_NAME),expirationTime,cleanWaitTime,edgeStoreCacheSize);
                indexStore = new ExpirationKCVSCache(indexStoreRaw,getMetricsCacheName(INDEXSTORE_NAME),expirationTime,cleanWaitTime,indexStoreCacheSize);
            } else {
                edgeStore = new NoKCVSCache(edgeStoreRaw);
                indexStore = new NoKCVSCache(indexStoreRaw);
            }

            //Just open them so that they are cached
            txLogManager.openLog(SYSTEM_TX_LOG_NAME);
            managementLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
            txLogStore = new NoKCVSCache(storeManager.openDatabase(SYSTEM_TX_LOG_NAME));


            //Open global configuration
            KeyColumnValueStore systemConfigStore = storeManagerLocking.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
            KCVSConfigurationBuilder kcvsConfigurationBuilder = new KCVSConfigurationBuilder();
            systemConfig = kcvsConfigurationBuilder.buildGlobalConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return storeManagerLocking.beginTransaction(StandardBaseTransactionConfig.of(
                            configuration.get(TIMESTAMP_PROVIDER),
                            storeFeatures.getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws BackendException {
                    //Do nothing, storeManager is closed explicitly by Backend
                }
            },systemConfigStore,configuration);
            userConfig = kcvsConfigurationBuilder.buildConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return storeManagerLocking.beginTransaction(StandardBaseTransactionConfig.of(configuration.get(TIMESTAMP_PROVIDER)));
                }

                @Override
                public void close() throws BackendException {
                    //Do nothing, storeManager is closed explicitly by Backend
                }
            },systemConfigStore,USER_CONFIGURATION_IDENTIFIER,configuration);

        } catch (BackendException e) {
            throw new JanusGraphException("Could not initialize backend", e);
        }
    }

    /**
     * Get information about all registered {@link IndexProvider}s.
     *
     * @return
     */
    public Map<String, IndexInformation> getIndexInformation() {
        return Collections.unmodifiableMap(new HashMap<>(indexes));
    }
//
//    public IndexProvider getIndexProvider(String name) {
//        return indexes.get(name);
//    }

    public KCVSLog getSystemTxLog() {
        try {
            return txLogManager.openLog(SYSTEM_TX_LOG_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not re-open transaction log", e);
        }
    }

    public Log getSystemMgmtLog() {
        try {
            return managementLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not re-open management log", e);
        }
    }

    public StandardScanner.Builder buildEdgeScanJob() {
        return buildStoreIndexScanJob(EDGESTORE_NAME);
    }

    public StandardScanner.Builder buildGraphIndexScanJob() {
        return buildStoreIndexScanJob(INDEXSTORE_NAME);
    }

    private StandardScanner.Builder buildStoreIndexScanJob(String storeName) {
        TimestampProvider provider = configuration.get(TIMESTAMP_PROVIDER);
        ModifiableConfiguration jobConfig = buildJobConfiguration();
        jobConfig.set(JOB_START_TIME,provider.getTime().toEpochMilli());
        return scanner.build()
                .setStoreName(storeName)
                .setTimestampProvider(provider)
                .setJobConfiguration(jobConfig)
                .setGraphConfiguration(configuration)
                .setNumProcessingThreads(1)
                .setWorkBlockSize(this.configuration.get(PAGE_SIZE));
    }

    public JanusGraphManagement.IndexJobFuture getScanJobStatus(Object jobId) {
        return scanner.getRunningJob(jobId);
    }

    public Log getUserLog(String identifier) throws BackendException {
        return userLogManager.openLog(getUserLogName(identifier));
    }

    public static String getUserLogName(String identifier) {
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier));
        return USER_LOG_PREFIX +identifier;
    }

    public KCVSConfiguration getGlobalSystemConfig() {
        return systemConfig;
    }

    public KCVSConfiguration getUserConfiguration() {
        return userConfig;
    }

    private String getMetricsCacheName(String storeName) {
        if (!configuration.get(BASIC_METRICS)) return null;
        return configuration.get(METRICS_MERGE_STORES) ? METRICS_MERGED_CACHE : storeName + METRICS_CACHE_SUFFIX;
    }

    public KCVSLogManager getKCVSLogManager(String logName) {
        Preconditions.checkArgument(configuration.restrictTo(logName).get(LOG_BACKEND).equalsIgnoreCase(LOG_BACKEND.getDefaultValue()));
        return (KCVSLogManager)getLogManager(logName);
    }

    public LogManager getLogManager(String logName) {
        return getLogManager(configuration, logName, storeManager);
    }

    private static LogManager getLogManager(Configuration config, String logName, KeyColumnValueStoreManager sm) {
        assert config!=null;
        Configuration logConfig = config.restrictTo(logName);
        String backend = logConfig.get(LOG_BACKEND);
        if (backend.equalsIgnoreCase(LOG_BACKEND.getDefaultValue())) {
            return new KCVSLogManager(sm,logConfig);
        } else {
            LogManager lm = getImplementationClass(logConfig,logConfig.get(LOG_BACKEND),REGISTERED_LOG_MANAGERS);
            Preconditions.checkNotNull(lm);
            return lm;
        }

    }

    public static KeyColumnValueStoreManager getStorageManager(Configuration storageConfig) {
        StoreManager manager = getImplementationClass(storageConfig, storageConfig.get(STORAGE_BACKEND),
                StandardStoreManager.getAllManagerClasses());
        if (manager instanceof OrderedKeyValueStoreManager) {
            Map<String, Integer> keyLength = new HashMap<>(3);
            keyLength.put(EDGESTORE_NAME, 8);
            keyLength.put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, 8);
            keyLength.put(storageConfig.get(IDS_STORE_NAME), 8);
            keyLength = Collections.unmodifiableMap(keyLength);
            manager = new OrderedKeyValueStoreManagerAdapter((OrderedKeyValueStoreManager) manager, keyLength);
        }
        Preconditions.checkArgument(manager instanceof KeyColumnValueStoreManager,"Invalid storage manager: %s",manager.getClass());
        return (KeyColumnValueStoreManager) manager;
    }

    private static Map<String, IndexProvider> getIndexes(Configuration config) {
        Set<String> containedIndexNamespaces = config.getContainedNamespaces(INDEX_NS);
        Map<String, IndexProvider> indexesMap = new HashMap<>(containedIndexNamespaces.size());
        for (String index : containedIndexNamespaces) {
            Preconditions.checkArgument(StringUtils.isNotBlank(index), "Invalid index name [%s]", index);
            log.info("Configuring index [{}]", index);
            IndexProvider provider = getImplementationClass(config.restrictTo(index), config.get(INDEX_BACKEND,index),
                    StandardIndexProvider.getAllProviderClasses());
            Preconditions.checkNotNull(provider);
            indexesMap.put(index, provider);
        }
        return Collections.unmodifiableMap(indexesMap);
    }

    public static <T> T getImplementationClass(Configuration config, String className, Map<String, String> registeredImplementations) {
        if (registeredImplementations.containsKey(className.toLowerCase())) {
            className = registeredImplementations.get(className.toLowerCase());
        }

        return ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }


    /**
     * Returns the configured {@link IDAuthority}.
     *
     * @return
     */
    public IDAuthority getIDAuthority() {
        return Preconditions.checkNotNull(idAuthority, "Backend has not yet been initialized");
    }

    /**
     * Returns the {@link StoreFeatures} of the configured backend storage engine.
     *
     * @return
     */
    public StoreFeatures getStoreFeatures() {
        return storeFeatures;
    }

    public Class<? extends KeyColumnValueStoreManager> getStoreManagerClass() {
        return storeManager.getClass();
    }

    public StoreManager getStoreManager() {
        return storeManager;
    }

    /**
     * Returns the {@link IndexFeatures} of all configured index backends
     */
    public Map<String,IndexFeatures> getIndexFeatures() {
        return indexes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getFeatures()));
    }

    /**
     * Opens a new transaction against all registered backend system wrapped in one {@link BackendTransaction}.
     *
     * @return
     * @throws BackendException
     */
    public BackendTransaction beginTransaction(TransactionConfiguration configuration, KeyInformation.Retriever indexKeyRetriever) throws BackendException {

        StoreTransaction tx = storeManagerLocking.beginTransaction(configuration);

        // Cache
        CacheTransaction cacheTx = new CacheTransaction(tx, storeManagerLocking, bufferSize, maxWriteTime, configuration.hasEnabledBatchLoading());

        // Index transactions
        final Map<String, IndexTransaction> indexTx = new HashMap<>(indexes.size());
        for (Map.Entry<String, IndexProvider> entry : indexes.entrySet()) {
            indexTx.put(entry.getKey(), new IndexTransaction(entry.getValue(), indexKeyRetriever.get(entry.getKey()), configuration, maxWriteTime));
        }

        return new BackendTransaction(cacheTx, configuration, storeFeatures,
                edgeStore, indexStore, txLogStore,
                maxReadTime, indexTx, threadPool);
    }

    public synchronized void close() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            managementLogManager.close();
            txLogManager.close();
            userLogManager.close();

            scanner.close();
            if (edgeStore != null) edgeStore.close();
            if (indexStore != null) indexStore.close();
            if (idAuthority != null) idAuthority.close();
            if (systemConfig != null) systemConfig.close();
            if (userConfig != null) userConfig.close();
            storeManager.close();
            if(threadPool != null) {
            	threadPool.shutdown();
            }
            //Indexes
            for (IndexProvider index : indexes.values()) index.close();
        } else {
            log.debug("Backend {} has already been closed or cleared", this);
        }
    }

    /**
     * Clears the storage of all registered backend data providers. This includes backend storage engines and index providers.
     * <p>
     * IMPORTANT: Clearing storage means that ALL data will be lost and cannot be recovered.
     *
     * @throws BackendException
     */
    public synchronized void clearStorage() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            managementLogManager.close();
            txLogManager.close();
            userLogManager.close();

            scanner.close();
            edgeStore.close();
            indexStore.close();
            idAuthority.close();
            systemConfig.close();
            userConfig.close();
            storeManager.clearStorage();
            storeManager.close();
            //Indexes
            for (IndexProvider index : indexes.values()) {
                index.clearStorage();
                index.close();
            }
        } else {
            log.debug("Backend {} has already been closed or cleared", this);
        }
    }

    private ModifiableConfiguration buildJobConfiguration() {

        return new ModifiableConfiguration(JOB_NS,
            new CommonsConfiguration(new BaseConfiguration()),
            BasicConfiguration.Restriction.NONE);
    }

    public static ConfigOption<?> getOptionForShorthand(String shorthand) {
        if (null == shorthand)
            return null;

        shorthand = shorthand.toLowerCase();

        for (StandardStoreManager m : STORE_SHORTHAND_OPTIONS.keySet()) {
            if (m.getShorthands().contains(shorthand))
                return STORE_SHORTHAND_OPTIONS.get(m);
        }

        return null;
    }

    private static Locker openManagedLocker(String classname, String lockerName) {
        try {
            Class c = Class.forName(classname);
            Constructor constructor = c.getConstructor();
            Object instance = constructor.newInstance();
            Method method = c.getMethod("openLocker", String.class);
            Object o = method.invoke(instance, lockerName);
            return (Locker) o;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find implementation class: " + classname);
        } catch (InstantiationException | ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate implementation: " + classname, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not find method when configuring locking for: " + classname,e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not access method when configuring locking for: " + classname,e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Could not invoke method when configuring locking for: " + classname,e);
        }
    }
}
