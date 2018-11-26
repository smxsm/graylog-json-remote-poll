package de.shoptimax;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

import org.apache.commons.lang3.StringUtils;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.*;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
// import com.jayway.jsonpath.JsonPath;
// import com.jayway.jsonpath.ReadContext;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URL;
import java.net.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;

import de.shoptimax.util.BasicAuthInterceptor;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Response;

import java.text.NumberFormat;
import java.text.ParseException;

/**
 * Main class, launches the Graylog input and starts a periodic HttpClient Monitor
 * polling remote JSON URIs to store the received GELF data.
 */
public class JRPInputTransport implements Transport {

    private static final Logger LOGGER = LoggerFactory.getLogger(JRPInputTransport.class.getName());
    private static final String CK_CONFIG_URL = "configURL";
    private static final String CK_CONFIG_LABEL = "configLabel";
    private static final String CK_CONFIG_HEADERS_TO_SEND = "configHeadersToSend";
    private static final String CK_CONFIG_USER_NAME = "configUsername";
    private static final String CK_CONFIG_PASSWORD = "configPassword";
    private static final String CK_CONFIG_TIMEOUT = "configTimeout";
    private static final String CK_CONFIG_TIMEOUT_UNIT = "configTimeoutUnit";
    private static final String CK_CONFIG_INTERVAL = "configInterval";
    private static final String CK_CONFIG_INTERVAL_UNIT = "configIntervalUnit";
    private static final String CK_CONFIG_HEADERS_TO_RECORD = "configHeadersToRecord";
    private static final String CK_CONFIG_LOG_RESPONSE_BODY = "configLogResponseBody";
    private static final String CK_CONFIG_HTTP_PROXY = "configHttpProxy";

    private final Configuration configuration;
    private final MetricRegistry metricRegistry;
    private ServerStatus serverStatus;
    private ScheduledExecutorService executorService;
    private ScheduledFuture future;
    private MessageInput messageInput;
    private OkHttpClient httpClient;
    private final Builder httpClientBuilder;

    @AssistedInject
    public JRPInputTransport(@Assisted Configuration configuration,
                             MetricRegistry metricRegistry,
                             ServerStatus serverStatus,
                             OkHttpClient httpClient) {
        this.configuration = configuration;
        this.metricRegistry = metricRegistry;
        this.serverStatus = serverStatus;
        this.httpClientBuilder = httpClient.newBuilder();
    }

    @Override
    public void setMessageAggregator(CodecAggregator codecAggregator) {

    }

    @Override
    public void launch(MessageInput messageInput) throws MisfireException {
        this.messageInput = messageInput;
        JRPInputConfig jrpConfig = new JRPInputConfig();
        jrpConfig.setUrl(configuration.getString(CK_CONFIG_URL));
        jrpConfig.setLabel(configuration.getString(CK_CONFIG_LABEL));

        String proxyUri = configuration.getString(CK_CONFIG_HTTP_PROXY);
        if (proxyUri != null && !proxyUri.isEmpty()) {
            jrpConfig.setHttpProxyUri(URI.create(proxyUri));
        }

        String requestHeaders = configuration.getString(CK_CONFIG_HEADERS_TO_SEND);
        if (StringUtils.isNotEmpty(requestHeaders)) {
            jrpConfig.setRequestHeadersToSend(
                    requestHeaders.split(","));
        }
        jrpConfig.setUsername(configuration.getString(CK_CONFIG_USER_NAME));
        jrpConfig.setPassword(configuration.getString(CK_CONFIG_PASSWORD));
        jrpConfig.setExecutionInterval(configuration.getInt(CK_CONFIG_INTERVAL));
        jrpConfig.setTimeout(configuration.getInt(CK_CONFIG_TIMEOUT));
        jrpConfig.setTimeoutUnit(TimeUnit.valueOf(configuration.getString(CK_CONFIG_TIMEOUT_UNIT)));
        jrpConfig.setIntervalUnit(TimeUnit.valueOf(configuration.getString(CK_CONFIG_INTERVAL_UNIT)));

        jrpConfig.setLogResponseBody(configuration.getBoolean(CK_CONFIG_LOG_RESPONSE_BODY));

        String responseHeaders = configuration.getString(CK_CONFIG_HEADERS_TO_RECORD);
        if (StringUtils.isNotEmpty(responseHeaders)) {
            jrpConfig.setResponseHeadersToRecord(
                    responseHeaders.split(","));
        }

        // now, configure and start!
        configBuilder(jrpConfig);
        startMonitoring(jrpConfig);
    }

    /**
     * Prepare the HttpClient builder - set basic auth,
     * proxy, timeouts, ...
     * @param configuration The JRPInputConfig holding the config values
     */
    private void configBuilder (JRPInputConfig configuration) {
        // set timeout
        if (configuration.getTimeout() > 0) {
            httpClientBuilder.connectTimeout(configuration.getTimeout(), configuration.getTimeoutUnit());
        }
        // basic auth
        httpClientBuilder.addInterceptor(new BasicAuthInterceptor(configuration.getUsername(), configuration.getPassword()));
        // set proxy
        URI proxyUri = configuration.getHttpProxyUri();
        if (proxyUri != null) {
            Proxy proxy = new Proxy(Proxy.Type.SOCKS,
                    new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()));
            httpClientBuilder.proxy(proxy);
        }
        LOGGER.debug("HttpClient Builder configured.");
        this.httpClient = httpClientBuilder.build();
    }

    @Override
    public void stop() {

        if (future != null) {
            future.cancel(true);
        }

        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    /**
     * Start threaded monitor task
     * @param config The JRPInputConfig holding the config values
     */
    private void startMonitoring(JRPInputConfig config) {
        executorService = Executors.newSingleThreadScheduledExecutor();
        long initalDelayMs = TimeUnit.MILLISECONDS.convert(Math.round(Math.random() * 60), TimeUnit.SECONDS);
        long executionIntervalMs = TimeUnit.MILLISECONDS.convert(config.getExecutionInterval(), config.getIntervalUnit());
        LOGGER.info("startMonitoring, initalDelayMs: " + initalDelayMs + " executionIntervalMs:" + executionIntervalMs);
        future = executorService.scheduleAtFixedRate(new MonitorTask(config, messageInput, httpClient), initalDelayMs,
                executionIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public MetricSet getMetricSet() {
        return null;
    }

    /**
     * Internal class doing the heavy work
     */
    private static class MonitorTask implements Runnable {
        private JRPInputConfig config;
        private MessageInput messageInput;
        private ObjectMapper mapper;
        private OkHttpClient httpClient;

        public MonitorTask(JRPInputConfig config, MessageInput messageInput, OkHttpClient okhttpClient) {
            this.config = config;
            this.messageInput = messageInput;
            this.mapper = new ObjectMapper();
            this.httpClient = okhttpClient;
        }

        @Override
        public void run() {
            //send to http server
            try {

                long startTime = System.currentTimeMillis();
                long time;
                Map<String, Object> eventdata = Maps.newHashMap();
                eventdata.put("_jrp_input_url", config.getUrl());
                eventdata.put("_label", config.getLabel());
                try {
                    LOGGER.debug("Running new request for URL {}", config.getUrl());
                    // set headers
                    final Headers.Builder headersBuilder = new Headers.Builder()
                            .add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                    if (config.getRequestHeadersToSend() != null) {
                        for (String header : config.getRequestHeadersToSend()) {
                            String tokens[] = header.split(":");
                            headersBuilder.set(tokens[0], tokens[1]);
                        }
                    }
                    Headers headers = headersBuilder.build();
                    LOGGER.debug("Added custom headers to request: {}", headers.toString());
                    // construct Request
                    final okhttp3.Request request = new okhttp3.Request.Builder()
                            .get()
                            .url(config.getUrl())
                            .headers(headers)
                            .build();
                    // get response
                    final Response response = httpClient.newCall(request).execute();

                    long endTime = System.currentTimeMillis();
                    time = endTime - startTime;

                    eventdata.put("host", response.request().url().host());
                    eventdata.put("_jrp_input_status", response.code());
                    eventdata.put("_jrp_input_statusLine", response.message());
                    String responseBodyStr = new String(response.body().bytes());
                    eventdata.put("_jrp_input_responseSize", responseBodyStr.length());
                    if (config.isLogResponseBody()) {
                        eventdata.put("full_message", responseBodyStr);
                    }
                    if (config.getResponseHeadersToRecord() != null) {
                        for (String header : config.getResponseHeadersToRecord()) {
                            eventdata.put("_" + header, response.header(header));
                        }
                    }

                    // get JSON from response body and fill eventdata!
                    String json = responseBodyStr;
                    // TODO use JsonPath to get custom root element (defined in config) so that an array of messages could be parsed
                    // Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);
                    // ReadContext ctx = JsonPath.parse(json);
                    // String version = ctx.read("$.version");

                    Map<String, Object> map = mapper.readValue(json, new TypeReference<Map<String,Object>>(){});
                    NumberFormat nf = NumberFormat.getInstance();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        String key = entry.getKey();
                        Object value = entry.getValue();
                        try {
                            // we have many numeric (custom) fields, so
                            // try to convert to number, e.g. for "timestamp" and "level"
                            LOGGER.debug("Parsing key <{}> and value {} to class {}", key, value.toString(), nf.parse(value.toString()).getClass().getName());
                            eventdata.put(key, nf.parse(value.toString()));
                        } catch (ParseException e) {
                            // if failed, it is probably a string :)
                            eventdata.put(key, value.toString());
                        }
                    }
                } catch (IOException e) {
                    eventdata.put("host", new URL(config.getUrl()).getHost());
                    eventdata.put("short_message", "Request failed :" + e.getMessage());
                    eventdata.put("_jrp_input_responseSize", 0);
                    long endTime = System.currentTimeMillis();
                    time = endTime - startTime;
                    //In case of connection timeout we get an execution exception with root cause as timeoutexception
                    if (e.getCause() instanceof TimeoutException) {
                        LOGGER.warn("Timeout while executing request for URL " + config.getUrl(), e);
                        eventdata.put("_jrp_input_status", 998);
                    } else if (e.getCause() instanceof ConnectException) {
                        //In case of connect exception we get an execution exception with root cause as connectexception
                        LOGGER.warn("Exception while executing request for URL " + config.getUrl(), e);
                        eventdata.put("_jrp_input_status", 999);
                    } else {
                        //Any other exception..
                        LOGGER.warn("Exception while executing request for URL " + config.getUrl(), e);
                        eventdata.put("_jrp_input_status", 997);
                    }
                }
                eventdata.put("_jrp_input_responseTime", time);

                //publish to graylog server
                ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                mapper.writeValue(byteStream, eventdata);
                messageInput.processRawMessage(new RawMessage(byteStream.toByteArray()));
                byteStream.close();

            } catch (IOException e) {
                LOGGER.error("Exception while executing request for URL " + config.getUrl(), e);
            }
        }
    }

    @FactoryClass
    public interface Factory extends Transport.Factory<JRPInputTransport> {

        @Override
        JRPInputTransport create(Configuration configuration);

        @Override
        Config getConfig();

    }

    @ConfigClass
    public static class Config implements Transport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest cr = new ConfigurationRequest();
            cr.addField(new TextField(CK_CONFIG_URL,
                    "URL to call",
                    "",
                    ""));
            cr.addField(new TextField(CK_CONFIG_LABEL,
                    "Label",
                    "",
                    "Label to identify this request"));

            cr.addField(new TextField(CK_CONFIG_HEADERS_TO_SEND,
                    "Additional HTTP headers",
                    "",
                    "Add a comma separated list of additional HTTP headers to send. For example: Accept: application/json, X-Requester: Graylog2",
                    ConfigurationField.Optional.OPTIONAL));

            cr.addField(new TextField(CK_CONFIG_USER_NAME,
                    "HTTP Basic Auth Username",
                    "",
                    "Username for HTTP Basic Authentication",
                    ConfigurationField.Optional.OPTIONAL));
            cr.addField(new TextField(CK_CONFIG_PASSWORD,
                    "HTTP Basic Auth Password",
                    "",
                    "Password for HTTP Basic Authentication",
                    ConfigurationField.Optional.OPTIONAL,
                    TextField.Attribute.IS_PASSWORD));

            cr.addField(new NumberField(CK_CONFIG_INTERVAL,
                    "Interval",
                    1,
                    "Time between between requests",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            Map<String, String> timeUnits = DropdownField.ValueTemplates.timeUnits();
            //Do not add nano seconds and micro seconds
            timeUnits.remove(TimeUnit.NANOSECONDS.toString());
            timeUnits.remove(TimeUnit.MICROSECONDS.toString());

            cr.addField(new DropdownField(
                    CK_CONFIG_INTERVAL_UNIT,
                    "Interval time unit",
                    TimeUnit.MINUTES.toString(),
                    timeUnits,
                    ConfigurationField.Optional.NOT_OPTIONAL
            ));


            cr.addField(new NumberField(CK_CONFIG_TIMEOUT,
                    "Timeout",
                    20,
                    "Timeout for requests",
                    ConfigurationField.Optional.NOT_OPTIONAL));

            cr.addField(new DropdownField(
                    CK_CONFIG_TIMEOUT_UNIT,
                    "Timeout time unit",
                    TimeUnit.SECONDS.toString(),
                    timeUnits,
                    ConfigurationField.Optional.NOT_OPTIONAL
            ));


            cr.addField(new TextField(CK_CONFIG_HTTP_PROXY,
                    "HTTP Proxy URI",
                    "",
                    "URI of HTTP Proxy to be used if required e.g. http://myproxy:8888",
                    ConfigurationField.Optional.OPTIONAL));


            cr.addField(new TextField(CK_CONFIG_HEADERS_TO_RECORD,
                    "Response headers to log",
                    "",
                    "Comma separated response headers to log. For example: Accept,Server,Expires",
                    ConfigurationField.Optional.OPTIONAL));

            cr.addField(new BooleanField(CK_CONFIG_LOG_RESPONSE_BODY,
                    "Log full response body",
                    false,
                    "Select if the complete response body needs to be logged as part of message"));

            return cr;
        }
    }

    public static void main(String args[]) {
        JRPInputConfig config = new JRPInputConfig();
        config.setUrl("https://www.graylog.org");
        MonitorTask monitorTask = new MonitorTask(config,null, null);
        monitorTask.run();
    }
}
