/**
 * Copyright 2013 Netherlands Forensic Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.minvenj.nfi.storm.kafka;

import backtype.storm.spout.Scheme;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static nl.minvenj.nfi.storm.kafka.util.ConfigUtils.createKafkaConfig;

public class RichKafkaSpout extends KafkaSpout {
    protected String groupId;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    public RichKafkaSpout(String groupId, String topic) {
        super(topic);
        this.groupId = groupId;
    }

    public RichKafkaSpout(String groupId, String topic, Scheme serializationScheme) {
        super(topic, serializationScheme);
        this.groupId = groupId;
    }
    @Override
    protected void createConsumer(final Map<String, Object> config) {
        final Properties consumerConfig = createKafkaConfig(config);
        consumerConfig.setProperty("group.id", groupId);

        LOG.info("connecting kafka client to zookeeper at {} as client group {}",
                consumerConfig.getProperty("zookeeper.connect"),
                consumerConfig.getProperty("group.id"));
        _consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));
    }
}
