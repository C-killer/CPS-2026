package fr.sorbonne_u.publication;

import java.time.Duration;
import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.messages.WindmillFilter;
import fr.sorbonne_u.meteo.MeteoAlert;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.meteo.RectangularRegion;
import fr.sorbonne_u.meteo.WindData;
import fr.sorbonne_u.publication.components.*;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class CVM_Audit1_filtre extends AbstractCVM {

	public CVM_Audit1_filtre() throws Exception {
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
				Windmill_before.class.getCanonicalName(),
				new Object[] {
						"windmill-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						// new MessageFilter(null, null, null) // match-all for demo
						new WindmillFilter()
				});

		// Meteo station 1 (publisher): strong wind from north, speed 42
		WindData wd1 = new WindData(new Position(2.5, 48.8), 0, 42);
		Message m1 = new Message(wd1, Instant.now());
		m1.putProperty("type", "wind");
		m1.putProperty("speed", wd1.force()); // 42.0

		AbstractComponent.createComponent(
				MeteoStation_before.class.getCanonicalName(),
				new Object[] {
						"station1-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						m1
				});

		// Meteo station 2 (publisher): weak wind from south, speed 10
		WindData wd2 = new WindData(new Position(3.0, 47.5), 0, -10);
		Message m2 = new Message(wd2, Instant.now());
		m2.putProperty("type", "wind");
		m2.putProperty("speed", wd2.force()); // 10.0

		AbstractComponent.createComponent(
				MeteoStation_before.class.getCanonicalName(),
				new Object[] {
						"station2-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						m2
				});

		// Meteo office (publisher): orange storm alert over Paris region
		MeteoAlert alert = new MeteoAlert(
				MeteoAlertI.AlertType.STORM,
				MeteoAlertI.Level.ORANGE,
				new RectangularRegion[] { new RectangularRegion(new Position(0, 45), new Position(10, 52)) },
				Instant.now(),
				Duration.ofHours(6));
		Message office = new Message(alert, Instant.now());
		office.putProperty("type", "alert");
		office.putProperty("level", "orange");

		AbstractComponent.createComponent(
				MeteoOffice_before.class.getCanonicalName(),
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
			CVM_Audit1_filtre cvm = new CVM_Audit1_filtre();
			cvm.startStandardLifeCycle(5000L);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}