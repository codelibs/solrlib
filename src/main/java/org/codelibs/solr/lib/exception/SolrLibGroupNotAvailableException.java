package org.codelibs.solr.lib.exception;

/**
 * Group is not available.
 * 
 * @author shinsuke
 *
 */
public class SolrLibGroupNotAvailableException extends SolrLibException {

    private static final long serialVersionUID = 1L;

    public SolrLibGroupNotAvailableException(final String groupName) {
        super("ESL0006", new Object[] { groupName });
    }

}
