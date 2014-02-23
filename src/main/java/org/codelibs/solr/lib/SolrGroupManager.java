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
import org.codelibs.core.msg.MessageFormatter;
import org.codelibs.core.util.DynamicProperties;
import org.codelibs.core.util.StringUtil;
import org.codelibs.solr.lib.exception.SolrLibException;
import org.codelibs.solr.lib.policy.QueryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr server group manager.
 * 
 * @author shinsuke
 *
 */
public class SolrGroupManager {
    private static final Logger logger = LoggerFactory
            .getLogger(SolrGroupManager.class);

    protected Map<String, SolrGroup> solrGroupMap = new LinkedHashMap<String, SolrGroup>();

    protected Timer monitorTimer;

    protected String selectGroupName;

    protected String updateGroupName;

    protected DynamicProperties solrProperties;

    protected long monitoringInterval = 60 * 1000L;

    public long getMonitoringInterval() {
        return monitoringInterval;
    }

    public void setMonitoringInterval(final long monitoringInterval) {
        this.monitoringInterval = monitoringInterval;
    }

    public void init() {
        if (solrProperties == null) {
            throw new SolrLibException("ESL0010");
        }

        selectGroupName = solrProperties.getProperty(
                SolrLibConstants.SELECT_GROUP, StringUtil.EMPTY);
        updateGroupName = solrProperties.getProperty(
                SolrLibConstants.UPDATE_GROUP, StringUtil.EMPTY);

        // check server name
        if (solrGroupMap.get(selectGroupName) == null) {
            // clear
            selectGroupName = null;
            updateGroupName = null;
        }

        if (selectGroupName == null) {
            synchronized (solrProperties) {
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

                solrProperties.setProperty(SolrLibConstants.SELECT_GROUP,
                        selectGroupName);
                solrProperties.setProperty(SolrLibConstants.UPDATE_GROUP,
                        updateGroupName);
                solrProperties.store();
            }
        }

        if (monitorTimer != null) {
            monitorTimer.cancel();
        }
        monitorTimer = new Timer("SolrGroupMonitor");
        monitorTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                for (final Map.Entry<String, SolrGroup> entry : solrGroupMap
                        .entrySet()) {
                    try {
                        entry.getValue().updateStatus();
                    } catch (final Exception e) {
                        logger.info(MessageFormatter.getSimpleMessage(
                                "ISL0001", new Object[] { entry.getKey() }), e);
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
        case ROLLBACK:
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
        synchronized (solrProperties) {
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

            solrProperties.setProperty(SolrLibConstants.SELECT_GROUP,
                    selectGroupName);
            solrProperties.setProperty(SolrLibConstants.UPDATE_GROUP,
                    updateGroupName);
            solrProperties.store();
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
                serverNameList.add(groupEntry.getKey()
                        + SolrLibConstants.GROUP_SERVER_SEPARATOR
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

    public DynamicProperties getSolrProperties() {
        return solrProperties;
    }

    public void setSolrProperties(final DynamicProperties groupStatusProperties) {
        solrProperties = groupStatusProperties;
    }

}
