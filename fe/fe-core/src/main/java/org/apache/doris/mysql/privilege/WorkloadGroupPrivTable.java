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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

public class WorkloadGroupPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(WorkloadGroupPrivTable.class);

    public void getPrivs(String workloadGroupName, PrivBitSet savedPrivs) {
        // need check all entries, because may have 2 entries match workloadGroupName,
        // For example, if the workloadGroupName is g1, there are two entry `%` and `g1` compound requirements
        List<PrivEntry> entries = getEntries();
        if (Objects.isNull(entries) || entries.isEmpty()) {
            return;
        }
        for (PrivEntry entry : entries) {
            WorkloadGroupPrivEntry workloadGroupPrivEntry = (WorkloadGroupPrivEntry) entry;
            if (workloadGroupPrivEntry.getWorkloadGroupPattern().match(workloadGroupName)) {
                savedPrivs.or(workloadGroupPrivEntry.getPrivSet());
            }
        }
    }
}
