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
package org.apache.dubbo.registry.client;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;

import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY;

public class ServiceDiscoveryRegistryFactory extends AbstractRegistryFactory {

    @Override
    protected Registry createRegistry(URL url) {
        // service-discovery-registry协议
        if (UrlUtils.hasServiceDiscoveryRegistryProtocol(url)) {
            // 将协议转换成原来的协议
            String protocol = url.getParameter(REGISTRY_KEY, DEFAULT_REGISTRY);
            url = url.setProtocol(protocol).removeParameter(REGISTRY_KEY);
        }
        // 创建ServiceDiscoverRegistry
        return new ServiceDiscoveryRegistry(url, applicationModel);
    }

}
