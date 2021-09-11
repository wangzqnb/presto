/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

import javax.inject.Inject;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.facebook.presto.server.PrestoServer.DatasourceAction;

import static com.facebook.presto.server.PrestoServer.updateDatasourcesAnnouncement;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/**
 * 静态加载catalog
 */
public class StaticCatalogStore
{
    private static final Logger log = Logger.get(StaticCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();

    @Inject
    public StaticCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config)
    {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()));
    }

    public StaticCatalogStore(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs)
    {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception
    {
        loadCatalogs(ImmutableMap.of());
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception
    {
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }

        additionalCatalogs.forEach(this::loadCatalog);

        catalogsLoaded.set(true);

        //开启catalog监控线程
        new Thread(() -> {
            try {
                log.info(">>>>>>>>>> Catalog watcher thread start <<<<<<<<<<");
                startCatalogWatcher(catalogConfigurationDir);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void loadCatalog(File file)
            throws Exception
    {
        String catalogName = Files.getNameWithoutExtension(file.getName());

        log.info("-- Loading catalog properties %s --", file);
        Map<String, String> properties = loadProperties(file);
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

        loadCatalog(catalogName, properties);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties)
    {
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", catalogName);

        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("connector.name")) {
                connectorName = entry.getValue();
            }
            else {
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }

        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);

        connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    /**
     *动态更改catalog（增、删、改）
     */
    private void startCatalogWatcher(File catalogConfigurationDir) throws IOException, InterruptedException
    {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Paths.get(catalogConfigurationDir.getAbsolutePath()).register(
                watchService, StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY);
        while (true) {
            WatchKey key = watchService.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                    log.info("New file in catalog directory : " + event.context());
                    Path newCatalog = (Path) event.context();
                    addCatalog(newCatalog);
                }
                else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                    log.info("Delete file from catalog directory : " + event.context());
                    Path deletedCatalog = (Path) event.context();
                    deleteCatalog(deletedCatalog);
                }
                else if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                    log.info("Modify file from catalog directory : " + event.context());
                    Path modifiedCatalog = (Path) event.context();
                    modifyCatalog(modifiedCatalog);
                }
            }
            boolean valid = key.reset();
            if (!valid) {
                break;
            }
        }
    }

    /**
     *增加catalog
     */
    private void addCatalog(Path catalogPath)
    {
        File file = new File(catalogConfigurationDir, catalogPath.getFileName().toString());
        if (file.isFile() && file.getName().endsWith(".properties")) {
            try {
                loadCatalog(file);
                updateDatasourcesAnnouncement(Files.getNameWithoutExtension(catalogPath.getFileName().toString()), DatasourceAction.ADD);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *删除catalog
     */
    private void deleteCatalog(Path catalogPath)
    {
        if (catalogPath.getFileName().toString().endsWith(".properties")) {
            String catalogName = Files.getNameWithoutExtension(catalogPath.getFileName().toString());
            log.info("-- Removing catalog %s", catalogName);
            connectorManager.dropConnection(catalogName);
            updateDatasourcesAnnouncement(catalogName, DatasourceAction.DELETE);
        }
    }

    /**
     *修改catalog
     */
    private void modifyCatalog(Path catalogPath)
    {
        deleteCatalog(catalogPath);
        addCatalog(catalogPath);
    }


}
