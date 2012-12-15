/*
 * Copyright 2012-2012 the CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.codelibs.solr.lib;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.solr.client.solrj.SolrServer;
import org.codelibs.core.CoreLibConstants;
import org.codelibs.core.util.DynamicProperties;
import org.codelibs.core.util.StringUtil;
import org.codelibs.solr.lib.exception.SolrLibException;
import org.codelibs.solr.lib.policy.QueryType;
import org.seasar.util.log.Logger;

/**
 * Solr server group manager.
 * 
 * 
 * @author shinsuke
 *
 */
public class SolrGroupManager {
    private static final Logger logger = Logger
            .getLogger(SolrGroupManager.class);

    private static final String NAME_SEPARATOR = ":";

    protected static final String SELECT_GROUP = "select.group";

    protected static final String UPDATE_GROUP = "update.group";

    protected Map<String, SolrGroup> solrGroupMap = new LinkedHashMap<String, SolrGroup>();

    protected Timer monitorTimer = new Timer("SolrGroupMonitor");

    protected String selectGroupName;

    protected String updateGroupName;

    protected DynamicProperties groupStatusProperties;

    protected long monitoringInterval = 60 * 1000L;

    public long getMonitoringInterval() {
        return monitoringInterval;
    }

    public void setMonitoringInterval(final long monitoringInterval) {
        this.monitoringInterval = monitoringInterval;
    }

    public void init() {
        if (groupStatusProperties == null) {
            throw new SolrLibException("ESL0010");
        }

        selectGroupName = groupStatusProperties.getProperty(SELECT_GROUP,
                CoreLibConstants.EMPTY_STRING);
        updateGroupName = groupStatusProperties.getProperty(UPDATE_GROUP,
                CoreLibConstants.EMPTY_STRING);

        // check server name
        if (solrGroupMap.get(selectGroupName) == null) {
            // clear
            selectGroupName = null;
            updateGroupName = null;
        }

        if (selectGroupName == null) {
            synchronized (groupStatusProperties) {
                if (selectGroupName != null) {
                    return;
                }

                final int numOfGroup = solrGroupMap.size();
                if (numOfGroup <= 0) {
                    throw new SolrLibException("ESL0009");
                }

                final Set<String> nameSet = solrGroupMap.keySet();
                if (nameSet.size() == 1) {
                    final Iterator<String> itr = nameSet.iterator();
                    selectGroupName = itr.next();
                    updateGroupName = selectGroupName;
                } else {
                    final Iterator<String> itr = nameSet.iterator();
                    selectGroupName = itr.next();
                    updateGroupName = itr.next();
                }

                groupStatusProperties
                        .setProperty(SELECT_GROUP, selectGroupName);
                groupStatusProperties
                        .setProperty(UPDATE_GROUP, updateGroupName);
                groupStatusProperties.store();
            }
        }

        monitorTimer.cancel();
        monitorTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                for (final Map.Entry<String, SolrGroup> entry : solrGroupMap
                        .entrySet()) {
                    try {
                        entry.getValue().updateStatus();
                    } catch (final Exception e) {
                        logger.log(
                                Logger.format("ISL0001",
                                        new Object[] { entry.getKey() }), e);
                    }
                }
            }
        }, monitoringInterval, monitoringInterval);
    }

    public void destory() {
        monitorTimer.cancel();

    }

    public SolrGroup getSolrGroup(final QueryType queryType) {
        switch (queryType) {
        case ADD:
        case COMMIT:
        case DELETE:
        case OPTIMIZE:
            return getSolrGroup(updateGroupName);
        case QUERY:
        case REQUEST:
            return getSolrGroup(selectGroupName);
        case PING:
        default:
            throw new SolrLibException("ESL0009");
        }
    }

    public SolrGroup getSolrGroup(final String solrServerGroupName) {

        final SolrGroup solrGroup = solrGroupMap.get(solrServerGroupName);
        if (solrGroup == null) {
            throw new SolrLibException("ESL0008",
                    new Object[] { solrServerGroupName });
        }

        return solrGroup;
    }

    public void applyNewSolrGroup() {
        synchronized (groupStatusProperties) {
            selectGroupName = updateGroupName;
            final Set<String> nameSet = solrGroupMap.keySet();
            final String[] names = nameSet.toArray(new String[nameSet.size()]);
            int num;
            for (num = 0; num < names.length; num++) {
                if (names[num].equals(selectGroupName)) {
                    break;
                }
            }
            // count up
            num++;
            if (num >= names.length) {
                num = 0;
            }
            updateGroupName = names[num];

            groupStatusProperties.setProperty(SELECT_GROUP, selectGroupName);
            groupStatusProperties.setProperty(UPDATE_GROUP, updateGroupName);
            groupStatusProperties.store();
        }
    }

    public String[] getSolrGroupNames() {
        final Set<String> nameSet = solrGroupMap.keySet();
        return nameSet.toArray(new String[nameSet.size()]);
    }

    public String[] getSolrServerNames() {
        final List<String> serverNameList = new ArrayList<String>();
        for (final Map.Entry<String, SolrGroup> groupEntry : solrGroupMap
                .entrySet()) {
            for (final Map.Entry<String, SolrServer> serverEntry : groupEntry
                    .getValue().solrServerMap.entrySet()) {
                serverNameList.add(groupEntry.getKey() + NAME_SEPARATOR
                        + serverEntry.getKey());
            }
        }
        return serverNameList.toArray(new String[serverNameList.size()]);
    }

    public void addSolrGroup(final SolrGroup solrGroup) {
        final String name = solrGroup.getGroupName();
        if (StringUtil.isBlank(name)) {
            throw new SolrLibException("ESL0007");
        }
        solrGroupMap.put(name, solrGroup);
    }

    public DynamicProperties getGroupStatusProperties() {
        return groupStatusProperties;
    }

    public void setGroupStatusProperties(
            final DynamicProperties groupStatusProperties) {
        this.groupStatusProperties = groupStatusProperties;
    }

}
