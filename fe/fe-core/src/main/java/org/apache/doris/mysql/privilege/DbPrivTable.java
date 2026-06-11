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

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/*
 * DbPrivTable saves all database level privs
 */
public class DbPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(DbPrivTable.class);

    /*
     * Return first priv which match the user@host on ctl.db.* The returned priv will be
     * saved in 'savedPrivs'.
     */

    public void getPrivs(String ctl, String db, PrivBitSet savedPrivs) {
        List<PrivEntry> entries = getEntries();
        if (Objects.isNull(entries) || entries.isEmpty()) {
            return;
        }

        Function<PrivEntry, DbPrivEntry> matchFunc = entry -> {
            try {
                DbPrivEntry dbPrivEntry = (DbPrivEntry) entry;

                // check catalog
                if (!dbPrivEntry.isAnyCtl() && !dbPrivEntry.getCtlPattern().match(ctl)) {
                    return null;
                }

                // check db
                // dbPrivEntry.getDbPattern() is always constructed by string as of form: 'xxx_db'
                if (!dbPrivEntry.isAnyDb() && !dbPrivEntry.getDbPattern().match(db)) {
                    return null;
                }

                return dbPrivEntry;
            } catch (Exception e) {
                LOG.warn("Privilege check failed when invoking getPrivs, ctl:{}, db:{}, entry:{}",
                        ctl, db, entry, e);
                throw new IllegalStateException("Failed to match privilege rule: " + entry, e);
            }
        };
        DbPrivEntry matchedEntry = doPrivMatch(entries, matchFunc);
        // Finally set privilege
        if (Objects.nonNull(matchedEntry)) {
            savedPrivs.or(matchedEntry.getPrivSet());
        }
    }

    public boolean hasPrivsOfCatalog(String ctl) {
        List<PrivEntry> entries = getEntries();
        if (Objects.isNull(entries) || entries.isEmpty()) {
            return false;
        }

        Function<PrivEntry, Boolean> matchFunc =  entry -> {
            try {
                DbPrivEntry dbPrivEntry = (DbPrivEntry) entry;

                // check catalog
                Preconditions.checkState(!dbPrivEntry.isAnyCtl());
                if (dbPrivEntry.getCtlPattern().match(ctl)) {
                    return true;
                }
                return null;
            } catch (Exception e) {
                LOG.warn("Privilege check failed when invoking hasPrivsOfCatalog, ctl:{} entry:{}", ctl,  entry, e);
                throw new IllegalStateException("Failed to match privilege rule: " + entry, e);
            }
        };

        Boolean isMatched = doPrivMatch(entries, matchFunc);

        return Objects.nonNull(isMatched) && isMatched;
    }

}
