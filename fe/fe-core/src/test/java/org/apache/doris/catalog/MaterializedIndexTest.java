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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.common.FeConstants;
import org.apache.doris.thrift.TStorageMedium;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MaterializedIndexTest {

    private MaterializedIndex index;
    private long indexId;

    private List<Column> columns;
    @Mocked
    private Env env;

    private FakeEnv fakeEnv;

    @Before
    public void setUp() {
        indexId = 10000;

        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.TINYINT), true, null, "", ""));
        columns.add(new Column("k2", ScalarType.createType(PrimitiveType.SMALLINT), true, null, "", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        index = new MaterializedIndex(indexId, IndexState.NORMAL);

        fakeEnv = new FakeEnv();
        FakeEnv.setEnv(env);
        FakeEnv.setMetaVersion(FeConstants.meta_version);
    }

    @Test
    public void getMethodTest() {
        Assert.assertEquals(indexId, index.getId());
    }

    @Test
    public void testGetTabletsReturnsImmutableSnapshot() {
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD);
        index.addTablet(new Tablet(1L), tabletMeta, true);

        List<Tablet> snapshot = index.getTablets();
        Assert.assertEquals(1, snapshot.size());

        index.addTablet(new Tablet(2L), tabletMeta, true);
        Assert.assertEquals(1, snapshot.size());
        Assert.assertEquals(2, index.getTablets().size());
        Assert.assertThrows(UnsupportedOperationException.class, () -> snapshot.add(new Tablet(3L)));
    }

    @Test
    public void testBulkPublishAfterInvertedIndexRegistration() {
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
        new Expectations(env) {
            {
                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, indexId, 1, TStorageMedium.HDD);
        List<Tablet> tablets = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            long tabletId = 100L + i;
            long backendId = 200L + i;
            Tablet tablet = new Tablet(tabletId);
            Replica replica = new Replica(300L + i, backendId, Replica.ReplicaState.NORMAL, 1L, 1);

            invertedIndex.addTablet(tabletId, tabletMeta);
            tablet.addReplica(replica);
            tablets.add(tablet);

            Assert.assertSame(replica, invertedIndex.getReplica(tabletId, backendId));
        }

        Assert.assertTrue(index.getTablets().isEmpty());
        index.appendTablets(tablets);

        Assert.assertEquals(4, index.getTablets().size());
        for (int i = 0; i < tablets.size(); i++) {
            Tablet tablet = tablets.get(i);
            Assert.assertSame(tablet, index.getTablets().get(i));
            Assert.assertSame(tablet, index.getTablet(tablet.getId()));
            Assert.assertEquals(tabletMeta, invertedIndex.getTabletMeta(tablet.getId()));
        }
    }

    @Test
    public void testConcurrentGetTabletsNeverThrows() throws InterruptedException {
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD);
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);

        Thread writer = new Thread(() -> {
            long id = 1000L;
            while (!stop.get()) {
                index.addTablet(new Tablet(id++), tabletMeta, true);
                if (index.getTablets().size() > 64) {
                    index.clearTabletsForRestore();
                }
            }
        });

        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 50000 && error.get() == null; i++) {
                    for (Tablet tablet : index.getTablets()) {
                        tablet.getId();
                    }
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                stop.set(true);
            }
        });

        writer.start();
        reader.start();
        reader.join();
        stop.set(true);
        writer.join();

        if (error.get() != null) {
            Assert.fail("getTablets() iteration threw under concurrent mutation: " + error.get());
        }
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        Path path = Files.createFile(Paths.get("./index"));
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));

        index.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        MaterializedIndex rIndex = MaterializedIndex.read(dis);
        Assert.assertEquals(index, rIndex);

        // 3. delete files
        dis.close();
        Files.deleteIfExists(path);
    }
}
