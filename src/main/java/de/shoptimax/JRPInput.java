package de.shoptimax;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.inputs.codecs.GelfCodec;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;

import javax.inject.Inject;

/**
 * This is the plugin. Your class should implement one of the existing plugin
 * interfaces. (i.e. AlarmCallback, MessageInput, MessageOutput)
 */
public class JRPInput extends MessageInput {

    private static final String NAME = "JSON Remote Polling";

    @AssistedInject
    public JRPInput(MetricRegistry metricRegistry, @Assisted Configuration configuration,
                    JRPInputTransport.Factory transportFactory, LocalMetricRegistry localRegistry,
                    GelfCodec.Factory codecFactory, Config config,
                    Descriptor descriptor, ServerStatus serverStatus) {
        super(metricRegistry, configuration, transportFactory.create(configuration), localRegistry,
                codecFactory.create(configuration), config, descriptor, serverStatus);
    }

    public interface Factory extends MessageInput.Factory<JRPInput> {
        @Override
        JRPInput create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "");
        }
    }

    public static class Config extends MessageInput.Config {
        @Inject
        public Config(JRPInputTransport.Factory transport, GelfCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}
