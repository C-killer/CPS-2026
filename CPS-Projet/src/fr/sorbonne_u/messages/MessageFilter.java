package fr.sorbonne_u.messages;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;

import java.io.Serializable;
import java.time.Instant;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

public class MessageFilter implements MessageFilterI {
	private static final long serialVersionUID = 1L;
	protected final PropertyFilterI[] propertyFilters;
	protected final PropertiesFilterI[] propertiesFilters;
	protected final TimeFilterI timeFilter;

	public MessageFilter(PropertyFilterI[] propertyFilters,
			PropertiesFilterI[] propertiesFilters,
			TimeFilterI timeFilter) {
		this.propertyFilters = (propertyFilters != null) ? propertyFilters : new PropertyFilterI[0];
		this.propertiesFilters = (propertiesFilters != null) ? propertiesFilters : new PropertiesFilterI[0];
		this.timeFilter = (timeFilter != null) ? timeFilter : new TimeFilter.JokerFilter();
	}

	@Override
	public PropertyFilterI[] getPropertyFilters() {
		return this.propertyFilters;
	}

	@Override
	public PropertiesFilterI[] getPropertiesFilters() {
		return this.propertiesFilters;
	}

	@Override
	public TimeFilterI getTimeFilter() {
		return this.timeFilter;
	}

	@Override
	public boolean match(MessageI message) {
		if (message == null)
			return false;

		if (!this.timeFilter.match(message.getTimeStamp())) {
			return false;
		}

		for (PropertyFilterI pf : this.propertyFilters) {
			String name = pf.getName();
			try {
				Serializable value = message.getPropertyValue(name);
				PropertyI tempProp = new Message.Property(name, value);
				if (!pf.match(tempProp)) {
					return false;
				}
			} catch (Exception e) {
				return false;
			}
		}

		for (PropertiesFilterI psf : this.propertiesFilters) {
			String[] names = psf.getMultiValuesFilter().getNames();
			PropertyI[] props = new PropertyI[names.length];

			for (int i = 0; i < names.length; i++) {
				String name = names[i];
				try {
					Serializable val = message.getPropertyValue(name);
					props[i] = new Message.Property(name, val);
				} catch (Exception e) {
					return false;
				}
			}
			if (!psf.match(props)) {
				return false;
			}
		}
		return true;
	}

	public static class ValueFilter implements MessageFilterI.ValueFilterI {
		private static final long serialVersionUID = 1L;

		private final Serializable value;

		public ValueFilter(Serializable value) {
			this.value = value;
		}

		@Override
		public boolean match(Serializable value) {
			return this.value.equals(value);
		}
	}

	public static class PropertyFilter implements MessageFilterI.PropertyFilterI {
		private static final long serialVersionUID = 1L;

		private final String name;
		private final ValueFilterI valueFilter;

		public PropertyFilter(String name, ValueFilterI valueFilter) {
			if (name == null || name.isEmpty())
				throw new IllegalArgumentException("Property name cannot be empty");
			if (valueFilter == null)
				throw new IllegalArgumentException("ValueFilter cannot be null");
			this.name = name;
			this.valueFilter = valueFilter;
		};

		@Override
		public String getName() {
			return name;
		}

		@Override
		public MessageFilterI.ValueFilterI getValueFilter() {
			return valueFilter;
		};

		@Override
		public boolean match(MessageI.PropertyI property) {
			return property != null
					&& property.getName().equals(this.name)
					&& this.valueFilter.match(property.getValue());
		}
	}

	public static class MultiValuesFilter implements MessageFilterI.MultiValuesFilterI {
		private static final long serialVersionUID = 1L;

		private final String[] names;

		public MultiValuesFilter(String[] names) {
			if (names == null || names.length == 0)
				throw new IllegalArgumentException("Names array cannot be empty");
			this.names = names;
		}

		@Override
		public String[] getNames() {
			return names;
		}

		@Override
		public boolean match(Serializable[] values) {
			if (values == null || values.length != names.length) {
				return false;
			}
			boolean flag = true;
			int i;
			for (i = 0; i < names.length; i++) {
				if (names[i] != values[i]) {
					flag = false;
					break;
				}
			}
			return flag;
		}
	}

	public static class PropertiesFilter implements PropertiesFilterI {
		private static final long serialVersionUID = 1L;

		private final MultiValuesFilterI multiValuesFilter;

		public PropertiesFilter(MultiValuesFilterI multiValuesFilter) {
			if (multiValuesFilter == null)
				throw new IllegalArgumentException("MultiValuesFilter cannot be null");
			this.multiValuesFilter = multiValuesFilter;
		}

		@Override
		public MultiValuesFilterI getMultiValuesFilter() {
			return this.multiValuesFilter;
		}

		@Override
		public boolean match(PropertyI[] properties) {
			if (properties == null)
				return false;

			Map<String, Serializable> tmp = new HashMap<>();

			for (PropertyI p : properties) {
				tmp.put(p.getName(), p.getValue());
			}

			Serializable[] values = new Serializable[multiValuesFilter.getNames().length];

			for (int i = 0; i < multiValuesFilter.getNames().length; i++) {
				Serializable v = tmp.get(multiValuesFilter.getNames()[i]);
				if (v == null) {
					return false;
				}
				values[i] = v;
			}

			return multiValuesFilter.match(values);
		}
	}

	public class TimeFilter {

		public static class JokerFilter implements TimeFilterI {
			@Override
			public boolean match(Instant timestamp) {
				return true;
			}
		}

		public static class BeforeFilter implements TimeFilterI {
			private Instant fin;

			public BeforeFilter(Instant fin) {
				this.fin = fin;
			}

			@Override
			public boolean match(Instant timestamp) {
				if (timestamp == null) {
					return false;
				}
				return !timestamp.isAfter(fin);
			}
		}

		public static class AfterFilter implements TimeFilterI {
			private Instant start;

			public AfterFilter(Instant start) {
				this.start = start;
			}

			@Override
			public boolean match(Instant timestamp) {
				if (timestamp == null) {
					return false;
				}
				return !timestamp.isBefore(start);
			}
		}

		public static class BetweenFilter implements TimeFilterI {
			private Instant start;
			private Instant fin;

			public BetweenFilter(Instant start, Instant fin) {
				this.start = start;
				this.fin = fin;
			}

			@Override
			public boolean match(Instant timestamp) {
				if (timestamp == null) {
					return false;
				}
				return !timestamp.isBefore(start) && !timestamp.isAfter(fin);
			}
		}
	}
}
