/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.config.deploy;

import org.apache.dubbo.common.config.ReferenceCache;
import org.apache.dubbo.common.deploy.AbstractDeployer;
import org.apache.dubbo.common.deploy.ApplicationDeployer;
import org.apache.dubbo.common.deploy.DeployState;
import org.apache.dubbo.common.deploy.ModuleDeployListener;
import org.apache.dubbo.common.deploy.ModuleDeployer;
import org.apache.dubbo.common.logger.ErrorTypeAwareLogger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.ServiceConfigBase;
import org.apache.dubbo.config.context.ModuleConfigManager;
import org.apache.dubbo.config.utils.SimpleReferenceCache;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ModuleServiceRepository;
import org.apache.dubbo.rpc.model.ProviderModel;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Export/refer services of module
 */
public class DefaultModuleDeployer extends AbstractDeployer<ModuleModel> implements ModuleDeployer {

    private static final ErrorTypeAwareLogger logger = LoggerFactory.getErrorTypeAwareLogger(DefaultModuleDeployer.class);

    private final List<CompletableFuture<?>> asyncExportingFutures = new ArrayList<>();

    private final List<CompletableFuture<?>> asyncReferringFutures = new ArrayList<>();

    private List<ServiceConfigBase<?>> exportedServices = new ArrayList<>();

    private ModuleModel moduleModel;

    private FrameworkExecutorRepository frameworkExecutorRepository;
    private ExecutorRepository executorRepository;

    private final ModuleConfigManager configManager;

    private final SimpleReferenceCache referenceCache;

    private ApplicationDeployer applicationDeployer;
    private CompletableFuture startFuture;
    private Boolean background;
    private Boolean exportAsync;
    private Boolean referAsync;
    private CompletableFuture<?> exportFuture;
    private CompletableFuture<?> referFuture;


    public DefaultModuleDeployer(ModuleModel moduleModel) {
        super(moduleModel);
        this.moduleModel = moduleModel;
        configManager = moduleModel.getConfigManager();
        frameworkExecutorRepository = moduleModel.getApplicationModel().getFrameworkModel().getBeanFactory().getBean(FrameworkExecutorRepository.class);
        executorRepository = moduleModel.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
        referenceCache = SimpleReferenceCache.newCache();
        applicationDeployer = DefaultApplicationDeployer.get(moduleModel);

        //load spi listener
        Set<ModuleDeployListener> listeners = moduleModel.getExtensionLoader(ModuleDeployListener.class).getSupportedExtensionInstances();
        for (ModuleDeployListener listener : listeners) {
            this.addDeployListener(listener);
        }
    }

    @Override
    public void initialize() throws IllegalStateException {
        // 检查状态
        if (initialized) {
            return;
        }
        // Ensure that the initialization is completed when concurrent calls
        synchronized (this) {
            // 双重检查
            if (initialized) {
                return;
            }
            // 加载ModuleModel的配置
            // 只有ModuleConfig、ProviderConfig、ConsumerConfig
            loadConfigs();

            // read ModuleConfig
            ModuleConfig moduleConfig = moduleModel.getConfigManager().getModule().orElseThrow(() -> new IllegalStateException("Default module config is not initialized"));
            exportAsync = Boolean.TRUE.equals(moduleConfig.getExportAsync());
            referAsync = Boolean.TRUE.equals(moduleConfig.getReferAsync());

            // start in background
            background = moduleConfig.getBackground();
            if (background == null) {
                // compatible with old usages
                background = isExportBackground() || isReferBackground();
            }

            initialized = true;
            if (logger.isInfoEnabled()) {
                logger.info(getIdentifier() + " has been initialized!");
            }
        }
    }

    @Override
    public Future start() throws IllegalStateException {
        // initialize，maybe deadlock applicationDeployer lock & moduleDeployer lock
        // 初始化ApplicationDeployer
        applicationDeployer.initialize();

        // 同步启动
        return startSync();
    }

    private synchronized Future startSync() throws IllegalStateException {
        // 检测服务状态是否满足启动条件
        if (isStopping() || isStopped() || isFailed()) {
            throw new IllegalStateException(getIdentifier() + " is stopping or stopped, can not start again");
        }

        try {
            // 服务正在启动或者已经启动完毕
            if (isStarting() || isStarted()) {
                return startFuture;
            }

            // 修改module状态为STARTING
            // 触发DeployListener监听器的onStarting方法
            onModuleStarting();

            // 初始化ModuleConfig
            initialize();

            // 导出服务
            // export services
            exportServices();

            // prepare application instance
            // exclude internal module to avoid wait itself
            if (moduleModel != moduleModel.getApplicationModel().getInternalModule()) {
                applicationDeployer.prepareInternalModule();
            }

            // 服务引用
            // refer services
            referServices();

            // if no async export/refer services, just set started
            if (asyncExportingFutures.isEmpty() && asyncReferringFutures.isEmpty()) {
                // 没有服务引用，也没有导出服务，直接将状态设置为STARTED
                onModuleStarted();
            } else {
                frameworkExecutorRepository.getSharedExecutor().submit(() -> {
                    try {
                        // wait for export finish
                        // 等待服务导出完成
                        waitExportFinish();
                        // wait for refer finish
                        // 等待服务引用完成
                        waitReferFinish();
                    } catch (Throwable e) {
                        logger.warn("wait for export/refer services occurred an exception", e);
                    } finally {
                        // 状态设置为STARTED
                        onModuleStarted();
                    }
                });
            }
        } catch (Throwable e) {
            onModuleFailed(getIdentifier() + " start failed: " + e, e);
            throw e;
        }
        return startFuture;
    }

    @Override
    public Future getStartFuture() {
        return startFuture;
    }

    private boolean hasExportedServices() {
        return configManager.getServices().size() > 0;
    }

    @Override
    public void stop() throws IllegalStateException {
        moduleModel.destroy();
    }

    @Override
    public void preDestroy() throws IllegalStateException {
        if (isStopping() || isStopped()) {
            return;
        }
        onModuleStopping();
    }

    @Override
    public synchronized void postDestroy() throws IllegalStateException {
        if (isStopped()) {
            return;
        }
        unexportServices();
        unreferServices();

        ModuleServiceRepository serviceRepository = moduleModel.getServiceRepository();
        if (serviceRepository != null) {
            List<ConsumerModel> consumerModels = serviceRepository.getReferredServices();

            for (ConsumerModel consumerModel : consumerModels) {
                try {
                    if (consumerModel.getDestroyRunner() != null) {
                        consumerModel.getDestroyRunner().run();
                    }
                } catch (Throwable t) {
                    logger.error("5-13", "there are problems with the custom implementation.", "", "Unable to destroy model: consumerModel.", t);
                }
            }

            List<ProviderModel> exportedServices = serviceRepository.getExportedServices();
            for (ProviderModel providerModel : exportedServices) {
                try {
                    if (providerModel.getDestroyRunner() != null) {
                        providerModel.getDestroyRunner().run();
                    }
                } catch (Throwable t) {
                    logger.error("5-13", "there are problems with the custom implementation.", "", "Unable to destroy model: providerModel.", t);
                }
            }
            serviceRepository.destroy();
        }
        onModuleStopped();
    }

    private void onModuleStarting() {
        // 修改状态为STARTING
        // 触发DeployerListener的onStarting方法
        setStarting();
        startFuture = new CompletableFuture();
        logger.info(getIdentifier() + " is starting.");
        // ApplicationDeployer通知module状态改变
        applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STARTING);
    }

    private void onModuleStarted() {
        try {
            // 检查状态是否是STARTING
            if (isStarting()) {
                // 将状态设置为STARTED
                // 触发DeployedListener的onStarted方法
                setStarted();
                logger.info(getIdentifier() + " has started.");
                // 确保状态设置为STARTED
                applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STARTED);
            }
        } finally {
            // complete module start future after application state changed
            completeStartFuture(true);
        }
    }

    private void onModuleFailed(String msg, Throwable ex) {
        try {
            setFailed(ex);
            logger.error("5-14", "", "", "Model start failed: " + msg, ex);
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STARTED);
        } finally {
            completeStartFuture(false);
        }
    }

    private void completeStartFuture(boolean value) {
        if (startFuture != null && !startFuture.isDone()) {
            startFuture.complete(value);
        }
        if (exportFuture != null && !exportFuture.isDone()) {
            exportFuture.cancel(true);
        }
        if (referFuture != null && !referFuture.isDone()) {
            referFuture.cancel(true);
        }
    }

    private void onModuleStopping() {
        try {
            setStopping();
            logger.info(getIdentifier() + " is stopping.");
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STOPPING);
        } finally {
            completeStartFuture(false);
        }
    }

    private void onModuleStopped() {
        try {
            setStopped();
            logger.info(getIdentifier() + " has stopped.");
            applicationDeployer.notifyModuleChanged(moduleModel, DeployState.STOPPED);
        } finally {
            completeStartFuture(false);
        }
    }

    private void loadConfigs() {
        // load module configs
        moduleModel.getConfigManager().loadConfigs();
        moduleModel.getConfigManager().refreshAll();
    }

    /**
     * 进行服务暴露
     */
    private void exportServices() {
        // 遍历每一份服务配置，导出服务
        for (ServiceConfigBase sc : configManager.getServices()) {
            exportServiceInternal(sc);
        }
    }

    private void exportServiceInternal(ServiceConfigBase sc) {
        ServiceConfig<?> serviceConfig = (ServiceConfig<?>) sc;
        if (!serviceConfig.isRefreshed()) {
            // 刷新服务陪孩子
            serviceConfig.refresh();
        }
        if (sc.isExported()) {
            // 服务已经暴露过
            return;
        }
        if (exportAsync || sc.shouldExportAsync()) {
            // 异步暴露
            ExecutorService executor = executorRepository.getServiceExportExecutor();
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    if (!sc.isExported()) {
                        // 进行服务暴露
                        sc.export();
                        // 添加到已暴露服务列表中
                        exportedServices.add(sc);
                    }
                } catch (Throwable t) {
                    logger.error("5-9", "", "", "Failed to async export service config: " + getIdentifier() + " , catch error : " + t.getMessage(), t);
                }
            }, executor);

            asyncExportingFutures.add(future);
        } else {
            // 同步暴露
            if (!sc.isExported()) {
                // 进行服务暴露
                sc.export();
                // 添加到已暴露服务列表中
                exportedServices.add(sc);
            }
        }
    }

    private void unexportServices() {
        exportedServices.forEach(sc -> {
            try {
                configManager.removeConfig(sc);
                sc.unexport();
            } catch (Exception ignored) {
                // ignored
            }
        });
        exportedServices.clear();

        asyncExportingFutures.forEach(future -> {
            if (!future.isDone()) {
                future.cancel(true);
            }
        });
        asyncExportingFutures.clear();
    }

    private void referServices() {
        // 处理每一个服务引用
        configManager.getReferences().forEach(rc -> {
            try {
                // 服务引用配置
                ReferenceConfig<?> referenceConfig = (ReferenceConfig<?>) rc;
                if (!referenceConfig.isRefreshed()) {
                    // 刷新服务引用配置
                    referenceConfig.refresh();
                }

                // 判断是否需要初始化
                if (rc.shouldInit()) {
                    // 是否需要异步初始化引用服务
                    if (referAsync || rc.shouldReferAsync()) {
                        ExecutorService executor = executorRepository.getServiceReferExecutor();
                        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                            try {
                                // 获取服务引用
                                referenceCache.get(rc);
                            } catch (Throwable t) {
                                logger.error("5-9", "", "", "Failed to async export service config: " + getIdentifier() + " , catch error : " + t.getMessage(), t);
                            }
                        }, executor);

                        asyncReferringFutures.add(future);
                    } else {
                        // 获取服务引用
                        referenceCache.get(rc);
                    }
                }
            } catch (Throwable t) {
                logger.error("5-15", "", "", "Model reference failed: " + getIdentifier() + " , catch error : " + t.getMessage(), t);
                referenceCache.destroy(rc);
                throw t;
            }
        });
    }

    private void unreferServices() {
        try {
            asyncReferringFutures.forEach(future -> {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            });
            asyncReferringFutures.clear();
            referenceCache.destroyAll();
        } catch (Exception ignored) {
        }
    }

    private void waitExportFinish() {
        try {
            logger.info(getIdentifier() + " waiting services exporting ...");
            exportFuture = CompletableFuture.allOf(asyncExportingFutures.toArray(new CompletableFuture[0]));
            exportFuture.get();
        } catch (Throwable e) {
            logger.warn(getIdentifier() + " export services occurred an exception: " + e.toString());
        } finally {
            logger.info(getIdentifier() + " export services finished.");
            asyncExportingFutures.clear();
        }
    }

    private void waitReferFinish() {
        try {
            logger.info(getIdentifier() + " waiting services referring ...");
            referFuture = CompletableFuture.allOf(asyncReferringFutures.toArray(new CompletableFuture[0]));
            referFuture.get();
        } catch (Throwable e) {
            logger.warn(getIdentifier() + " refer services occurred an exception: " + e.toString());
        } finally {
            logger.info(getIdentifier() + " refer services finished.");
            asyncReferringFutures.clear();
        }
    }

    @Override
    public boolean isBackground() {
        return background;
    }

    private boolean isExportBackground() {
        return moduleModel.getConfigManager().getProviders()
            .stream()
            .map(ProviderConfig::getExportBackground)
            .filter(k -> k != null && k)
            .findAny()
            .isPresent();
    }

    private boolean isReferBackground() {
        return moduleModel.getConfigManager().getConsumers()
            .stream()
            .map(ConsumerConfig::getReferBackground)
            .filter(k -> k != null && k)
            .findAny()
            .isPresent();
    }

    @Override
    public ReferenceCache getReferenceCache() {
        return referenceCache;
    }

    /**
     * Prepare for export/refer service, trigger initializing application and module
     */
    @Override
    public void prepare() {
        applicationDeployer.initialize();
        this.initialize();
    }

}
