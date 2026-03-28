package fr.sorbonne_u.publication.components;

import java.time.Instant;

import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.meteo.interfaces.WindDataI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.publication.Client;

/**
 * Windmill subscriber component.
 *
 * <p>
 * On receiving a {@link WindDataI} payload, the windmill:
 * <ol>
 *   <li>Discards the message if its timestamp is older than
 *       {@code timeWindowSeconds}.</li>
 *   <li>Discards the message if the emitting station is farther than
 *       {@code proximityThreshold}.</li>
 *   <li>Computes an inverse-distance-weighted contribution and updates its
 *       orientation to face against the resulting wind vector.</li>
 * </ol>
 * On receiving a {@link MeteoAlertI} payload:
 * <ol>
 *   <li>Ignores the alert if the windmill's own position is outside every
 *       affected region.</li>
 *   <li>Stops (safety mode) if the alert level is at or above
 *       {@code alertThresholdOrdinal} (default: ORANGE).</li>
 *   <li>Resumes normal operation if the alert level is GREEN or YELLOW
 *       (below the threshold).</li>
 * </ol>
 * </p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class Windmill extends Client {

	/** Default proximity radius: stations within this distance are considered. */
	public static final double DEFAULT_PROXIMITY        = 100.0;
	/** Default time window: messages older than this (seconds) are discarded. */
	public static final long   DEFAULT_TIME_WINDOW      = 3600L;
	/**
	 * Default alert threshold ordinal.
	 * {@code MeteoAlertI.Level.ORANGE.ordinal() == 2}.
	 */
	public static final int    DEFAULT_ALERT_THRESHOLD  = 2;

	// -------------------------------------------------------------------------
	// Configuration (immutable after construction)
	// -------------------------------------------------------------------------

	/** Geographic position of this windmill in the 2D Cartesian plane. */
	protected final Position position;
	/** Maximum distance (inclusive) from which wind data is accepted. */
	protected final double proximityThreshold;
	/** Maximum age in seconds of wind data to be considered. */
	protected final long timeWindowSeconds;
	/**
	 * Minimum {@link MeteoAlertI.Level} ordinal that triggers a safety stop.
	 * Alerts with ordinal strictly below this value (GREEN, YELLOW) signal
	 * that the emergency is over and the windmill should resume.
	 */
	protected final int alertThresholdOrdinal;

	// -------------------------------------------------------------------------
	// Mutable state
	// -------------------------------------------------------------------------

	/** Current facing direction (unit vector). Initial value: east (1, 0). */
	private double orientationX = 1.0;
	private double orientationY = 0.0;
	/** True when the windmill is in safety-stop mode due to an alert. */
	private boolean stopped = false;

	// =========================================================================
	// Constructors
	// =========================================================================

	/**
	 * Full constructor with position and tuning parameters.
	 */
	protected Windmill(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter,
			TestScenario scenario,
			Position position,
			double proximityThreshold,
			long timeWindowSeconds,
			int alertThresholdOrdinal) throws Exception {
		super(receivingInboundURI, brokerRegistrationInboundURI,
				serviceClass, channel, filter, null, scenario);
		this.position              = position;
		this.proximityThreshold    = proximityThreshold;
		this.timeWindowSeconds     = timeWindowSeconds;
		this.alertThresholdOrdinal = alertThresholdOrdinal;
	}

	/**
	 * Scenario constructor with default position (origin) and default parameters.
	 * Used by CVM scenario tests.
	 */
	protected Windmill(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter,
			TestScenario scenario) throws Exception {
		this(receivingInboundURI, brokerRegistrationInboundURI,
				serviceClass, channel, filter, scenario,
				new Position(0, 0),
				DEFAULT_PROXIMITY, DEFAULT_TIME_WINDOW, DEFAULT_ALERT_THRESHOLD);
	}

	/**
	 * No-scenario constructor with filter (for non-timed tests such as
	 * {@code CVM_PluginTest}).
	 */
	protected Windmill(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter) throws Exception {
		this(receivingInboundURI, brokerRegistrationInboundURI,
				serviceClass, channel, filter, null,
				new Position(0, 0),
				DEFAULT_PROXIMITY, DEFAULT_TIME_WINDOW, DEFAULT_ALERT_THRESHOLD);
	}

	/**
	 * Convenience constructor: FREE service + match-all filter, with scenario.
	 * Position defaults to origin.
	 */
	protected Windmill(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			String channel,
			TestScenario scenario) throws Exception {
		this(receivingInboundURI, brokerRegistrationInboundURI,
				RegistrationClass.FREE, channel,
				new MessageFilter(null, null, null), scenario,
				new Position(0, 0),
				DEFAULT_PROXIMITY, DEFAULT_TIME_WINDOW, DEFAULT_ALERT_THRESHOLD);
	}

	// =========================================================================
	// Application logic
	// =========================================================================

	/**
	 * Hook called by {@code ReceivingInbound} after subscription-plugin logging.
	 * Dispatches the message to domain-level handlers based on payload type.
	 */
	@Override
	public void onMessageReceived(String channel, MessageI message) {
		Object payload = message.getPayload();
		if (payload instanceof WindDataI) {
			processWindData((WindDataI) payload, message.getTimeStamp());
		} else if (payload instanceof MeteoAlertI) {
			processAlert((MeteoAlertI) payload);
		}
	}

	/**
	 * Process incoming wind data: check freshness and proximity, then update
	 * the windmill's orientation using an inverse-distance-weighted contribution.
	 *
	 * <p>Orientation is set to the negation of the normalised weighted wind
	 * vector so that the windmill faces into the wind.</p>
	 */
	private synchronized void processWindData(WindDataI wd, Instant msgTime) {

		// 1. Freshness check
		if (msgTime != null
				&& msgTime.isBefore(Instant.now().minusSeconds(timeWindowSeconds))) {
			System.out.printf("[%s] wind data discarded: age > %ds%n",
					receivingInboundURI, timeWindowSeconds);
			return;
		}

		// 2. Proximity check
		double dist = distanceTo(wd.getPosition());
		if (dist > proximityThreshold) {
			System.out.printf("[%s] wind data discarded: distance %.1f > threshold %.1f%n",
					receivingInboundURI, dist, proximityThreshold);
			return;
		}

		// 3. Inverse-distance weight (minimum distance 1.0 to avoid division by zero)
		double weight = 1.0 / Math.max(dist, 1.0);
		double wx = wd.xComponent() * weight;
		double wy = wd.yComponent() * weight;

		// 4. Orient against the wind: negate the normalised weighted vector
		double norm = Math.sqrt(wx * wx + wy * wy);
		if (norm > 1e-9) {
			this.orientationX = -wx / norm;
			this.orientationY = -wy / norm;
		}

		System.out.printf(
				"[%s] wind from %s  force=%.1f  dist=%.1f  → orientation=(%.3f, %.3f)%s%n",
				receivingInboundURI,
				wd.getPosition(),
				wd.force(),
				dist,
				orientationX,
				orientationY,
				stopped ? "  [STOPPED — safety mode]" : "");
	}

	/**
	 * Process an incoming meteo alert.
	 *
	 * <ul>
	 *   <li>If the windmill is outside every affected region: no action.</li>
	 *   <li>If level {@literal >=} threshold: enter safety stop.</li>
	 *   <li>If level {@literal <} threshold (GREEN / YELLOW): resume if stopped.</li>
	 * </ul>
	 */
	private synchronized void processAlert(MeteoAlertI alert) {

		// 1. Region check
		boolean inRegion = false;
		for (RegionI r : alert.getRegions()) {
			if (r.in(this.position)) {
				inRegion = true;
				break;
			}
		}

		if (!inRegion) {
			System.out.printf("[%s] alert %s: windmill outside affected region → no action%n",
					receivingInboundURI, alert.getLevel());
			return;
		}

		// 2. Level comparison via enum ordinal
		int lvl = levelOrdinal(alert.getLevel());

		if (lvl >= this.alertThresholdOrdinal) {
			if (!this.stopped) {
				this.stopped = true;
				System.out.printf("[%s] SAFETY STOP — alert level %s in region%n",
						receivingInboundURI, alert.getLevel());
			}
		} else {
			// GREEN (0) or YELLOW (1): all-clear signal
			if (this.stopped) {
				this.stopped = false;
				System.out.printf("[%s] RESUMING — alert downgraded to %s%n",
						receivingInboundURI, alert.getLevel());
			} else {
				System.out.printf("[%s] alert level %s in region — windmill already running%n",
						receivingInboundURI, alert.getLevel());
			}
		}
	}

	// =========================================================================
	// Helpers
	// =========================================================================

	/**
	 * Euclidean distance from this windmill's position to {@code other}.
	 * Returns 0 if the position cannot be cast to {@link Position}.
	 */
	private double distanceTo(fr.sorbonne_u.cps.meteo.interfaces.PositionI other) {
		if (!(other instanceof Position)) return 0.0;
		Position p = (Position) other;
		double dx = p.getX() - this.position.getX();
		double dy = p.getY() - this.position.getY();
		return Math.sqrt(dx * dx + dy * dy);
	}

	/**
	 * Map a {@link MeteoAlertI.LevelI} to an integer ordinal for comparison.
	 * Delegates to {@link MeteoAlertI.Level#ordinal()} when possible;
	 * defaults to ORANGE (2) for unknown implementations.
	 */
	private static int levelOrdinal(MeteoAlertI.LevelI level) {
		if (level instanceof MeteoAlertI.Level) {
			return ((MeteoAlertI.Level) level).ordinal();
		}
		return DEFAULT_ALERT_THRESHOLD; // safe default: treat as ORANGE
	}
}
