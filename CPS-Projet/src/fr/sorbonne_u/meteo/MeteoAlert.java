package fr.sorbonne_u.meteo;

import java.time.Duration;
import java.time.Instant;

import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

/**
 * A meteorological alert with type, severity level, affected regions,
 * start time, and duration.
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class MeteoAlert implements MeteoAlertI {

	private static final long serialVersionUID = 1L;

	protected final AlertTypeI alertType;
	protected final LevelI     level;
	protected final RegionI[]  regions;
	protected final Instant    startTime;
	protected final Duration   duration;

	/**
	 * Create a meteorological alert.
	 *
	 * @param alertType type of the alert (e.g. STORM, FLOODING).
	 * @param level     severity level (e.g. ORANGE, RED).
	 * @param regions   regions affected by this alert.
	 * @param startTime start time of the alert.
	 * @param duration  expected duration.
	 */
	public MeteoAlert(
			AlertTypeI alertType,
			LevelI level,
			RegionI[] regions,
			Instant startTime,
			Duration duration) {
		assert alertType != null : "alertType must not be null";
		assert level     != null : "level must not be null";
		assert regions   != null && regions.length > 0 : "regions must not be empty";
		assert startTime != null : "startTime must not be null";
		assert duration  != null : "duration must not be null";
		this.alertType = alertType;
		this.level     = level;
		this.regions   = regions.clone();
		this.startTime = startTime;
		this.duration  = duration;
	}

	@Override
	public AlertTypeI getAlertType() { return this.alertType; }

	@Override
	public LevelI getLevel()         { return this.level; }

	@Override
	public RegionI[] getRegions()    { return this.regions.clone(); }

	@Override
	public Instant getStartTime()    { return this.startTime; }

	@Override
	public Duration getDuration()    { return this.duration; }

	@Override
	public String toString() {
		return "MeteoAlert[type=" + alertType
				+ ", level=" + level
				+ ", start=" + startTime
				+ ", duration=" + duration + "]";
	}
}
