
public interface HealpixImpl {

	/** Provide the HEALPix number associated to a coord, for a given order
	    * @param order HEALPix order [0..MAXORDER]
	    * @param lon longitude (expressed in the Healpix frame)
	    * @param lat latitude (expressed in the Healpix frame)
	    * @return HEALPix number
	    * @throws Exception
	    */
	   public long ang2pix(int order,double lon, double lat) throws Exception;
	   
	   /** Provide the galactic coord associated to an HEALPix number, for a given order
	    * @param order HEALPix order [0..MAXORDER]
	    * @param npix HEALPix number
	    * @return spherical coord (lon,lat) (expressed in the Healpix frame)
	    * @throws Exception
	    */
	   public abstract double [] pix2ang(int order,long npix) throws Exception;
	   
	   /** Provide the list of HEALPix numbers fully covering a circle (for a specified order)
	    * @param order Healpix order
	    * @param lon    center longitude (expressed in the Healpix frame)
	    * @param lat    center latitude (expressed in the Healpix frame)
	    * @param radius circle radius (in degrees)
	    * @return
	    * @throws Exception
	    */
	   public long [] queryDisc(int order, double lon, double lat, double radius) throws Exception;

}
