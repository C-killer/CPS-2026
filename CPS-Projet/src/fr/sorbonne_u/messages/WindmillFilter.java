package fr.sorbonne_u.messages;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

/**
 * A filter that accepts wind messages whose speed is >= 30.
 * Used by windmill subscriber clients to select strong-wind readings.
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class WindmillFilter implements MessageFilterI {

	private static final long serialVersionUID = 1L;

	/** Minimum wind speed (inclusive) to pass the filter. */
	private static final double MIN_SPEED = 30.0;

	private final PropertyFilterI[] propertyFilters;

	public WindmillFilter() {
		this.propertyFilters = new PropertyFilterI[] {
				new MessageFilter.PropertyFilter(
						"speed",
						new MessageFilter.ValueFilter.GreaterThanFilter(MIN_SPEED - 1))
		};
	}

	@Override
	public boolean match(MessageI message) {
		if (message == null) return false;
		try {
			Object type     = message.getPropertyValue("type");
			Object speedObj = message.getPropertyValue("speed");

			if (!"wind".equals(type) || speedObj == null) return false;

			double speed;
			if (speedObj instanceof Number) {
				speed = ((Number) speedObj).doubleValue();
			} else {
				return false;
			}
			return speed >= MIN_SPEED;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public PropertyFilterI[] getPropertyFilters() {
		return this.propertyFilters.clone();
	}

	@Override
	public PropertiesFilterI[] getPropertiesFilters() {
		return new PropertiesFilterI[0];
	}

	@Override
	public TimeFilterI getTimeFilter() {
		return new MessageFilter.TimeFilter.JokerFilter();
	}
}
