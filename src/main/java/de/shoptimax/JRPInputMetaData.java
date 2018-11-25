package de.shoptimax;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * Implement the PluginMetaData interface here.
 */
public class JRPInputMetaData implements PluginMetaData {
    private static final String PLUGIN_PROPERTIES = "de.shoptimax.graylog-plugin-json-remote-poll/graylog-plugin.properties";

    @Override
    public String getUniqueId() {
        return "de.shoptimax.JRPInputPlugin";
    }

    @Override
    public String getName() {
        return "JRPInput";
    }

    @Override
    public String getAuthor() {
        return "Stefan Moises <moises@shoptimax.de>";
    }

    @Override
    public URI getURL() {
        return URI.create("https://github.com/smxsm/graylog-json-remote-poll");
    }

    @Override
    public Version getVersion() {
        return Version.fromPluginProperties(getClass(), PLUGIN_PROPERTIES, "version", Version.from(0, 0, 0, "unknown"));
    }

    @Override
    public String getDescription() {
        return "Graylog JSON Remote Polling (JRP) input plugin";
    }

    @Override
    public Version getRequiredVersion() {
        return Version.fromPluginProperties(getClass(), PLUGIN_PROPERTIES, "graylog.version", Version.from(0, 0, 0, "unknown"));
    }

    @Override
    public Set<ServerStatus.Capability> getRequiredCapabilities() {
        return Collections.emptySet();
    }
}
