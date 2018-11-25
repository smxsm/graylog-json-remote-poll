package de.shoptimax.util;

import java.io.IOException;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthInterceptor.class.getName());

    private String credentials;

    public BasicAuthInterceptor(String user, String password) {
        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            this.credentials = Credentials.basic(user, password);
        }
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        // if no auth data given, proceed original request
        if (StringUtils.isEmpty(credentials)) {
            LOGGER.debug("No basic auth data given");
            return chain.proceed(request);
        }
        LOGGER.debug("Adding basic auth header to request");
        Request authenticatedRequest = request.newBuilder()
                .header("Authorization", credentials).build();
        return chain.proceed(authenticatedRequest);
    }
}
