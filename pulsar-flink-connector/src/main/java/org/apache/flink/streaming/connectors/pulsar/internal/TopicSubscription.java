package org.apache.flink.streaming.connectors.pulsar.internal;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class TopicSubscription implements Serializable {

	private String topic;

	private SerializableRange range;

	private String subscriptionName;
}
