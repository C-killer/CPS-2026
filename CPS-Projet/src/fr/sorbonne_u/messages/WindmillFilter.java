package fr.sorbonne_u.messages;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;

public class WindmillFilter implements MessageFilterI {

	@Override
	public boolean match(MessageI message) {
		try {
			Object type = message.getPropertyValue("type");
			Object speedObj = message.getPropertyValue("speed");

			if (type == null || speedObj == null)
				return false;

			if (!"wind".equals(type))
				return false;

			if (!(speedObj instanceof Integer))
				return false;

			int speed = (Integer) speedObj;

			return speed >= 30;

		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public PropertyFilterI[] getPropertyFilters() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getPropertyFilters'");
	}

	@Override
	public PropertiesFilterI[] getPropertiesFilters() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getPropertiesFilters'");
	}

	@Override
	public TimeFilterI getTimeFilter() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'getTimeFilter'");
	}
}