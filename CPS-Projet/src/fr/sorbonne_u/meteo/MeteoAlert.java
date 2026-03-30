package fr.sorbonne_u.meteo;

import java.time.Duration;
import java.time.Instant;

import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

/**
 * Represents a complete weather warning. It contains: the warning type (e.g.
 * STORM, FLOODING), the severity level (e.g. ORANGE, RED), a list of affected
 * regions (RegionI[], i.e. the regions mentioned above), the start time of the
 * warning and its expected duration.
 * 表示一条完整的气象警报。里面包含了：警报类型(如暴风雨 STORM、洪水 FLOODING)、严重级别(如橙色 ORANGE、红色
 * RED)、受影响的区域列表(RegionI[]，即上面提到的区域)、警报开始时间和预计持续时间.
 * 
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class MeteoAlert implements MeteoAlertI {

	private static final long serialVersionUID = 1L;

	protected final AlertTypeI alertType;
	protected final LevelI level;
	protected final RegionI[] regions;
	protected final Instant startTime;
	protected final Duration duration;

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
		assert level != null : "level must not be null";
		assert regions != null && regions.length > 0 : "regions must not be empty";
		assert startTime != null : "startTime must not be null";
		assert duration != null : "duration must not be null";
		this.alertType = alertType;
		this.level = level;
		this.regions = regions.clone();
		this.startTime = startTime;
		this.duration = duration;
	}

	@Override
	public AlertTypeI getAlertType() {
		return this.alertType;
	}

	@Override
	public LevelI getLevel() {
		return this.level;
	}

	@Override
	public RegionI[] getRegions() {
		return this.regions.clone();
	}

	@Override
	public Instant getStartTime() {
		return this.startTime;
	}

	@Override
	public Duration getDuration() {
		return this.duration;
	}

	@Override
	public String toString() {
		return "MeteoAlert[type=" + alertType
				+ ", level=" + level
				+ ", start=" + startTime
				+ ", duration=" + duration + "]";
	}
}
