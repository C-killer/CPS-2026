package fr.sorbonne_u.meteo;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;

/**
 * Concrete implementation of a 2D Cartesian position.
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class Position implements PositionI {

	private static final long serialVersionUID = 1L;

	/** x coordinate. */
	protected final double x;
	/** y coordinate. */
	protected final double y;

	/**
	 * Create a position with the given coordinates.
	 *
	 * @param x x coordinate.
	 * @param y y coordinate.
	 */
	public Position(double x, double y) {
		this.x = x;
		this.y = y;
	}

	/** @return the x coordinate. */
	public double getX() { return this.x; }

	/** @return the y coordinate. */
	public double getY() { return this.y; }

	@Override
	public boolean equals(PositionI p) {
		if (p == null) return false;
		if (!(p instanceof Position)) return false;
		Position other = (Position) p;
		return Double.compare(this.x, other.x) == 0
				&& Double.compare(this.y, other.y) == 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!(obj instanceof PositionI)) return false;
		return this.equals((PositionI) obj);
	}

	@Override
	public int hashCode() {
		return Double.hashCode(x) * 31 + Double.hashCode(y);
	}

	@Override
	public String toString() {
		return "Position(" + x + ", " + y + ")";
	}
}
