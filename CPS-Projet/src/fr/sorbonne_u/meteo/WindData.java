package fr.sorbonne_u.meteo;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.WindDataI;

/**
 * Wind measurement data: position, direction vector, and force.
 *
 * <p>
 * Invariant: {@code force == Math.sqrt(xComponent * xComponent + yComponent * yComponent)}
 * </p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class WindData implements WindDataI {

	private static final long serialVersionUID = 1L;

	/** Position at which the wind was measured. */
	protected final Position position;
	/** x component of the wind vector. */
	protected final double xComponent;
	/** y component of the wind vector. */
	protected final double yComponent;

	/**
	 * Create wind data from position and vector components.
	 * The force is derived automatically as the Euclidean norm of the vector.
	 *
	 * @param position   measurement position.
	 * @param xComponent x component of the wind vector.
	 * @param yComponent y component of the wind vector.
	 */
	public WindData(Position position, double xComponent, double yComponent) {
		assert position != null : "position must not be null";
		this.position   = position;
		this.xComponent = xComponent;
		this.yComponent = yComponent;
	}

	@Override
	public PositionI getPosition() {
		return this.position;
	}

	@Override
	public double xComponent() {
		return this.xComponent;
	}

	@Override
	public double yComponent() {
		return this.yComponent;
	}

	/** Force is the Euclidean norm of the (x, y) wind vector. */
	@Override
	public double force() {
		return Math.sqrt(xComponent * xComponent + yComponent * yComponent);
	}

	@Override
	public String toString() {
		return "WindData[pos=" + position
				+ ", vec=(" + xComponent + ", " + yComponent + ")"
				+ ", force=" + String.format("%.2f", force()) + "]";
	}
}
