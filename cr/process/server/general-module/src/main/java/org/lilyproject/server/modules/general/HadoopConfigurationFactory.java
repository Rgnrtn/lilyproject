/*
 * Copyright 2010 Outerthought bvba
 *
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
package org.lilyproject.server.modules.general;

import org.apache.hadoop.conf.Configuration;
import org.kauriproject.conf.Conf;

public interface HadoopConfigurationFactory {
    Configuration getHBaseConf();

    Configuration getMapReduceConf();

    Configuration getHadoopConf();

    /**
     * Combines the default MR properties with those specified in the conf argument.
     */
    Configuration getMapReduceConf(Conf subConf);

    String getZooKeeperConnectString();

    int getZooKeeperSessionTimeout();
}
