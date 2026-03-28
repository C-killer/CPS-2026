package fr.sorbonne_u.messages;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class Message implements MessageI {
	private static final long serialVersionUID = 1L;

	public static class Property implements MessageI.PropertyI {
		private static final long serialVersionUID = 1L;

		private final String name;
		private final Serializable value;

		public Property(String name, Serializable value) {
			if (name == null || name.isEmpty()) {
				throw new IllegalArgumentException(
						"Property name must be non-null and non-empty.");
			}
			this.name = name;
			this.value = value;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public Serializable getValue() {
			return value;
		}
	}

	private Serializable payload;
	private final Instant timestamp;
	private final Map<String, MessageI.PropertyI> properties; // name → PropertyI

	public Message(Serializable payload) {
		this(payload, Instant.now());
	}

	public Message(Serializable payload, Instant timestamp) {
		this.payload = payload;
		this.timestamp = timestamp;
		this.properties = new HashMap<>();
	}

	@Override
	public boolean propertyExists(String name) {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException(
					"Property name must be non-null and non-empty.");
		}
		return properties.containsKey(name);
	}

	@Override
	public void putProperty(String name, Serializable value) {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException(
					"Property name must be non-null and non-empty.");
		}
		this.properties.put(name, new Property(name, value));
	}

	@Override
	public void removeProperty(String name) throws UnknownPropertyException {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException(
					"Property name must be non-null and non-empty.");
		}
		if (!this.properties.containsKey(name)) {
			throw new UnknownPropertyException(name);
		}
		this.properties.remove(name);
	}

	@Override
	public Serializable getPropertyValue(String name) throws UnknownPropertyException {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException(
					"Property name must be non-null and non-empty.");
		}
		MessageI.PropertyI property = this.properties.get(name);
		if (property == null) {
			throw new UnknownPropertyException(name);
		}
		return property.getValue();
	}

	@Override
	public PropertyI[] getProperties() {
		return this.properties.values().toArray(new PropertyI[0]);
	}

	@Override
	// keep timestamp
	public MessageI copy() {
		Message m = new Message(this.payload, this.timestamp);
		for (MessageI.PropertyI p : this.properties.values()) {
			m.properties.put(p.getName(), new Property(p.getName(), p.getValue()));
		}
		return m;
	}

	@Override
	public void setPayload(Serializable payload) {
		this.payload = payload;
	}

	@Override
	public Serializable getPayload() {
		return this.payload;
	}

	@Override
	public Instant getTimeStamp() {
		return this.timestamp;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Message{");
		sb.append("payload=").append(this.payload);
		sb.append(", time=").append(this.timestamp);
		sb.append(", properties={");

		boolean first = true;
		for (MessageI.PropertyI p : this.properties.values()) {
			if (!first)
				sb.append(", ");
			sb.append(p.getName()).append("=").append(p.getValue());
			first = false;
		}

		sb.append("}}");
		return sb.toString();
	}
}
