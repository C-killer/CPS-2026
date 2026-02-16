package fr.sorbonne_u.publication;

import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.messages.WindmillFilter;

public class CVM_Audit1 extends AbstractCVM {

	public CVM_Audit1() throws Exception {
		super();
	}

	public static final int NB_CHANNELS = 3;

	@Override
	public void deploy() throws Exception {
		// Broker
		AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] { NB_CHANNELS });

		// Windmill (subscriber)
		AbstractComponent.createComponent(
				Windmill.class.getCanonicalName(),
				new Object[] {
						"windmill-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						new MessageFilter(null, null, null) // match-all for demo
				// new WindmillFilter()
				});

		// Meteo station 1 (publisher): wind speed 42
		Message m1 = new Message("station1", Instant.now());
		m1.putProperty("type", "wind");
		m1.putProperty("speed", 42);

		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						"station1-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						m1
				});

		// Meteo station 2 (publisher): wind speed 10
		Message m2 = new Message("station2", Instant.now());
		m2.putProperty("type", "wind");
		m2.putProperty("speed", 10);

		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						"station2-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						m2
				});

		// Meteo office (publisher): alert message
		Message office = new Message("office", Instant.now());
		office.putProperty("type", "alert");
		office.putProperty("level", "orange");

		AbstractComponent.createComponent(
				MeteoOffice.class.getCanonicalName(),
				new Object[] {
						"office-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						office
				});

		super.deploy();
	}

	public static void main(String[] args) {
		try {
			CVM_Audit1 cvm = new CVM_Audit1();
			cvm.startStandardLifeCycle(5000L);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}