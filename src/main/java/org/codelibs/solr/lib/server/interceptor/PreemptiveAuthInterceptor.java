package org.codelibs.solr.lib.server.interceptor;

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.ContextAwareAuthScheme;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;

public class PreemptiveAuthInterceptor implements HttpRequestInterceptor {

    protected ContextAwareAuthScheme authScheme = new BasicScheme();

    @Override
    public void process(final HttpRequest request, final HttpContext context)
            throws HttpException, IOException {
        final AuthState authState = (AuthState) context
                .getAttribute(ClientContext.TARGET_AUTH_STATE);

        if (authState != null && authState.getAuthScheme() == null) {
            final CredentialsProvider credsProvider = (CredentialsProvider) context
                    .getAttribute(ClientContext.CREDS_PROVIDER);
            final HttpHost targetHost = (HttpHost) context
                    .getAttribute(ExecutionContext.HTTP_TARGET_HOST);
            final Credentials creds = credsProvider
                    .getCredentials(new AuthScope(targetHost.getHostName(),
                            targetHost.getPort()));
            if (creds == null) {
                throw new HttpException(
                        "No credentials for preemptive authentication");
            }
            request.addHeader(authScheme.authenticate(creds, request, context));
        }
    }

    public ContextAwareAuthScheme getAuthScheme() {
        return authScheme;
    }

    public void setAuthScheme(final ContextAwareAuthScheme authScheme) {
        this.authScheme = authScheme;
    }

}
