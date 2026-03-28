package fr.sorbonne_u.meteo;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

/**
 * A rectangular geographic region defined by two corners (bottom-left and
 * top-right) in Cartesian coordinates.
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class RectangularRegion implements RegionI {

	private static final long serialVersionUID = 1L;

	/** Bottom-left corner of the rectangle. */
	protected final Position bottomLeft;
	/** Top-right corner of the rectangle. */
	protected final Position topRight;

	/**
	 * Create a rectangular region.
	 *
	 * @param bottomLeft bottom-left corner (min x, min y).
	 * @param topRight   top-right corner (max x, max y).
	 */
	public RectangularRegion(Position bottomLeft, Position topRight) {
		assert bottomLeft.getX() <= topRight.getX()
				&& bottomLeft.getY() <= topRight.getY()
				: "bottomLeft must be below and to the left of topRight";
		this.bottomLeft = bottomLeft;
		this.topRight   = topRight;
	}

	@Override
	public boolean in(PositionI p) {
		if (!(p instanceof Position)) return false;
		Position pos = (Position) p;
		return pos.getX() >= bottomLeft.getX()
				&& pos.getX() <= topRight.getX()
				&& pos.getY() >= bottomLeft.getY()
				&& pos.getY() <= topRight.getY();
	}

	@Override
	public String toString() {
		return "RectangularRegion[" + bottomLeft + " -> " + topRight + "]";
	}
}
