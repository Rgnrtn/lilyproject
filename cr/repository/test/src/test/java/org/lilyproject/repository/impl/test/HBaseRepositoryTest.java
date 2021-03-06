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
package org.lilyproject.repository.impl.test;


import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.util.io.Closer;

public class HBaseRepositoryTest extends AbstractRepositoryTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        repoSetup.setupCore();
        repoSetup.setupRepository();

        idGenerator = repoSetup.getIdGenerator();
        repository = repoSetup.getRepository();
        typeManager = repoSetup.getTypeManager();

        setupTypes();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();
    }
    
    @Test
    public void testFieldTypeCacheInitialization() throws Exception {
        TypeManager newTypeManager = new HBaseTypeManager(repoSetup.getIdGenerator(), repoSetup.getHadoopConf(),
                repoSetup.getZk(), repoSetup.getHbaseTableFactory());
        assertEquals(fieldType1, newTypeManager.getFieldTypeByName(fieldType1.getName()));
        Closer.close(newTypeManager);
    }
}
