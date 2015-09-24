package org.codelibs.solr.lib.response;

import java.util.ArrayList;
import java.util.List;

public class CoreReloadResponse {
    private final List<ServerResposne> serverResposneList = new ArrayList<>();

    private int status = 0;

    public void addReloadResult(final String groupName, final String serverName,
            final int status) {
        if (status != 0) {
            this.status = 1;
        }
        serverResposneList
                .add(new ServerResposne(groupName, serverName, status));
    }

    public int getStatus() {
        return status;
    }

    public ServerResposne[] getServerResposnes() {
        return serverResposneList
                .toArray(new ServerResposne[serverResposneList.size()]);
    }

    public static class ServerResposne {

        private final String groupName;

        private final String serverName;

        private final int status;

        public ServerResposne(final String groupName, final String serverName,
                final int status) {
            this.groupName = groupName;
            this.serverName = serverName;
            this.status = status;
        }

        public String getGroupName() {
            return groupName;
        }

        public String getServerName() {
            return serverName;
        }

        public int getStatus() {
            return status;
        }

    }
}
