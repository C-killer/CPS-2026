package fr.sorbonne_u.publication;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI.FComponentTask;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.meteo.WindData;
import fr.sorbonne_u.publication.components.MeteoStation;
import fr.sorbonne_u.publication.components.Windmill;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Integration test demonstrating:
 * <ul>
 *   <li><b>TimeFilter.AfterFilter</b>: subscriber only receives messages with
 *       a recent timestamp; an artificially "old" message (2 days ago) is
 *       rejected.</li>
 *   <li><b>PropertiesFilter + MultiValuesFilter</b>: subscriber uses a
 *       cross-property constraint (type="wind" AND zone="north").</li>
 *   <li><b>Channel destruction</b>: a PREMIUM client creates a temporary
 *       channel, a subscriber joins it, one message is delivered, then the
 *       channel is destroyed via {@code destroyChannel}.</li>
 * </ul>
 *
 * Expected console output summary:
 * <pre>
 *  [time-filter-sub]  receives recentWindNorth  (timestamp now)   ← PASS
 *  [time-filter-sub]  receives recentWindSouth  (timestamp now)   ← PASS
 *  [time-filter-sub]  does NOT receive oldWindNorth (2 days ago)  ← FILTERED
 *
 *  [props-filter-sub] receives recentWindNorth  (north, wind)     ← PASS
 *  [props-filter-sub] receives oldWindNorth     (north, wind)     ← PASS (no time constraint)
 *  [props-filter-sub] does NOT receive recentWindSouth (south)    ← FILTERED
 *
 *  [temp-sub]         receives tempAlert before channel destroyed ← PASS
 *  [temp-destroyer]   destroys temp-channel at t+40s
 * </pre>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class CVM_FilterTest extends AbstractCVM {

	// Broker pre-creates channel0 only
	public static final int    NB_CHANNELS        = 1;

	public static final String CLOCK_URI           = "filter-demo-clock";
	public static final String START_INSTANT       = "2026-04-01T10:00:00Z";
	public static final String END_INSTANT         = "2026-04-01T10:02:00Z";
	public static final double ACCELERATION_FACTOR = 60.0;

	// Client URIs
	public static final String TIME_FILTER_SUB      = "time-filter-sub";
	public static final String PROPS_FILTER_SUB     = "props-filter-sub";
	public static final String TEMP_SUB             = "temp-sub";
	public static final String STATION_RECENT_NORTH = "station-recent-north";
	public static final String STATION_RECENT_SOUTH = "station-recent-south";
	public static final String STATION_OLD_NORTH    = "station-old-north";
	public static final String TEMP_DESTROYER       = "temp-destroyer";

	public static final String TEMP_CHANNEL = "temp-channel";

	public CVM_FilterTest() throws Exception {
		super();
	}

	// -------------------------------------------------------------------------
	// Filter factories
	// -------------------------------------------------------------------------

	/**
	 * TimeFilter: accept only messages whose timestamp is at most 60 seconds old.
	 * Messages created with {@code Instant.now()} at deploy() time will pass;
	 * messages with timestamp 2 days in the past will be rejected.
	 */
	protected static MessageFilterI recentTimeFilter() {
		return new MessageFilter(
				null,
				null,
				new MessageFilter.TimeFilter.AfterFilter(
						Instant.now().minusSeconds(60)));
	}

	/**
	 * PropertiesFilter: cross-property constraint requiring BOTH
	 * {@code type="wind"} AND {@code zone="north"} simultaneously.
	 * Demonstrates {@link MessageFilter.MultiValuesFilter}.
	 */
	protected static MessageFilterI northWindPropertiesFilter() {
		return new MessageFilter(
				null,
				new MessageFilterI.PropertiesFilterI[] {
						new MessageFilter.PropertiesFilter(
								new MessageFilter.MultiValuesFilter(
										new String[]       { "type", "zone"  },
										new Serializable[] { "wind", "north" }))
				},
				null);
	}

	// -------------------------------------------------------------------------
	// Scenario
	// -------------------------------------------------------------------------

	protected static TestScenario buildScenario() throws Exception {
		Instant start = Instant.parse(START_INSTANT);
		Instant end   = Instant.parse(END_INSTANT);

		Instant tCreate      = start.plusSeconds(3);   // TEMP_DESTROYER creates temp-channel
		Instant tSubTemp     = start.plusSeconds(10);  // TEMP_SUB subscribes to temp-channel
		Instant tSubCh0      = start.plusSeconds(15);  // filter subs subscribe to channel0
		Instant tPublish     = start.plusSeconds(25);  // stations publish to channel0
		Instant tPublishTemp = start.plusSeconds(30);  // TEMP_DESTROYER publishes to temp-channel
		Instant tDestroy     = start.plusSeconds(40);  // TEMP_DESTROYER destroys temp-channel

		return new TestScenario(CLOCK_URI, start, end, new TestStepI[] {

				// Step 1 — PREMIUM creator creates temp-channel
				new TestStep(CLOCK_URI, TEMP_DESTROYER, tCreate,
						(FComponentTask) owner ->
								((Client) owner).runScenarioCreateChannels()),

				// Step 2 — temp-sub subscribes to the newly-created temp-channel
				new TestStep(CLOCK_URI, TEMP_SUB, tSubTemp,
						(FComponentTask) owner ->
								((Client) owner).runScenarioSubscribe()),

				// Step 3 — filter subscribers subscribe to channel0
				new TestStep(CLOCK_URI, TIME_FILTER_SUB, tSubCh0,
						(FComponentTask) owner ->
								((Client) owner).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, PROPS_FILTER_SUB, tSubCh0,
						(FComponentTask) owner ->
								((Client) owner).runScenarioSubscribe()),

				// Step 4 — three stations publish: 2 recent + 1 old (2 days ago)
				new TestStep(CLOCK_URI, STATION_RECENT_NORTH, tPublish,
						(FComponentTask) owner ->
								((Client) owner).runScenarioPublish()),
				new TestStep(CLOCK_URI, STATION_RECENT_SOUTH, tPublish,
						(FComponentTask) owner ->
								((Client) owner).runScenarioPublish()),
				new TestStep(CLOCK_URI, STATION_OLD_NORTH, tPublish,
						(FComponentTask) owner ->
								((Client) owner).runScenarioPublish()),

				// Step 5 — TEMP_DESTROYER publishes one message to temp-channel
				new TestStep(CLOCK_URI, TEMP_DESTROYER, tPublishTemp,
						(FComponentTask) owner ->
								((Client) owner).runScenarioPublish()),

				// Step 6 — TEMP_DESTROYER destroys temp-channel
				new TestStep(CLOCK_URI, TEMP_DESTROYER, tDestroy,
						(FComponentTask) owner ->
								((Client) owner).runScenarioDestroyChannel()),
		});
	}

	// -------------------------------------------------------------------------
	// Deploy
	// -------------------------------------------------------------------------

	@Override
	public void deploy() throws Exception {
		long startTimeNanos = TimeUnit.MILLISECONDS.toNanos(
				System.currentTimeMillis() + 2000L);

		AbstractComponent.createComponent(ClocksServer.class.getCanonicalName(),
				new Object[] {
						CLOCK_URI,
						startTimeNanos,
						Instant.parse(START_INSTANT),
						ACCELERATION_FACTOR });

		final TestScenario scenario = buildScenario();

		// Broker — pre-creates channel0 only
		AbstractComponent.createComponent(Broker.class.getCanonicalName(),
				new Object[] { NB_CHANNELS });

		final String brokerURI = Broker.registrationPortURI();

		// ---- Build messages at deploy() time ----

		// Recent north wind (speed=42) — timestamp = now
		WindData wdNorthRecent = new WindData(new Position(0, 0), 0, 42);
		Message msgNorthRecent = new Message(wdNorthRecent, Instant.now());
		msgNorthRecent.putProperty("type", "wind");
		msgNorthRecent.putProperty("zone", "north");
		msgNorthRecent.putProperty("speed", wdNorthRecent.force());

		// Recent south wind (speed=38) — timestamp = now
		WindData wdSouthRecent = new WindData(new Position(0, 0), 0, -38);
		Message msgSouthRecent = new Message(wdSouthRecent, Instant.now());
		msgSouthRecent.putProperty("type", "wind");
		msgSouthRecent.putProperty("zone", "south");
		msgSouthRecent.putProperty("speed", wdSouthRecent.force());

		// OLD north wind (speed=35) — timestamp 2 days ago
		// Expected: rejected by TimeFilter.AfterFilter, accepted by PropertiesFilter
		WindData wdNorthOld = new WindData(new Position(0, 0), 0, 35);
		Message msgNorthOld = new Message(wdNorthOld,
				Instant.now().minus(Duration.ofDays(2)));
		msgNorthOld.putProperty("type", "wind");
		msgNorthOld.putProperty("zone", "north");
		msgNorthOld.putProperty("speed", wdNorthOld.force());

		// Temp-channel message
		Message msgTemp = new Message("temp-alert", Instant.now());
		msgTemp.putProperty("type", "temp");

		// ---- Subscribers ----

		// Subscriber 1: TimeFilter.AfterFilter — rejects messages older than 60s
		AbstractComponent.createComponent(Windmill.class.getCanonicalName(),
				new Object[] {
						TIME_FILTER_SUB, brokerURI,
						RegistrationClass.FREE, "channel0",
						recentTimeFilter(), scenario });

		// Subscriber 2: PropertiesFilter — type=wind AND zone=north
		AbstractComponent.createComponent(Windmill.class.getCanonicalName(),
				new Object[] {
						PROPS_FILTER_SUB, brokerURI,
						RegistrationClass.FREE, "channel0",
						northWindPropertiesFilter(), scenario });

		// Subscriber 3: subscribes to temp-channel (STANDARD, pass-all filter)
		AbstractComponent.createComponent(Windmill.class.getCanonicalName(),
				new Object[] {
						TEMP_SUB, brokerURI,
						RegistrationClass.STANDARD, TEMP_CHANNEL,
						new MessageFilter(null, null, null), scenario });

		// ---- Publishers ----

		AbstractComponent.createComponent(MeteoStation.class.getCanonicalName(),
				new Object[] {
						STATION_RECENT_NORTH, brokerURI,
						RegistrationClass.FREE, "channel0",
						msgNorthRecent, scenario });

		AbstractComponent.createComponent(MeteoStation.class.getCanonicalName(),
				new Object[] {
						STATION_RECENT_SOUTH, brokerURI,
						RegistrationClass.FREE, "channel0",
						msgSouthRecent, scenario });

		AbstractComponent.createComponent(MeteoStation.class.getCanonicalName(),
				new Object[] {
						STATION_OLD_NORTH, brokerURI,
						RegistrationClass.FREE, "channel0",
						msgNorthOld, scenario });

		// ---- Channel creator + publisher + destroyer (PREMIUM) ----
		// Steps: createChannel(t+3) → publish(t+30) → destroyChannel(t+40)
		AbstractComponent.createComponent(Client.class.getCanonicalName(),
				new Object[] {
						TEMP_DESTROYER, brokerURI,
						RegistrationClass.PREMIUM, TEMP_CHANNEL,
						new MessageFilter(null, null, null),
						msgTemp, scenario });

		super.deploy();
	}

	// -------------------------------------------------------------------------
	// Main
	// -------------------------------------------------------------------------

	public static void main(String[] args) {
		try {
			CVM_FilterTest cvm = new CVM_FilterTest();
			cvm.startStandardLifeCycle(10000L);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
