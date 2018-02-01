import healpix.essentials.*;

public final class Healpix implements HealpixImpl {
   
   /** Provide the HEALPix number associated to a coord, for a given order
    * @param order HEALPix order [0..MAXORDER]
    * @param lon longitude (expressed in the Healpix frame)
    * @param lat latitude (expressed in the Healpix frame)
    * @return HEALPix number
    * @throws Exception
    */
   public long ang2pix(int order,double lon, double lat) throws Exception {
      double theta = Math.PI/2. - lat/180.*Math.PI;
      double phi = lon/180.*Math.PI;
      return healpixBase[order].ang2pix(new Pointing(theta,phi));
   }
   
   /** Provide the spherical coord associated to an HEALPix number, for a given order
    * @param order HEALPix order [0..MAXORDER]
    * @param npix HEALPix number
    * @return coord (lon,lat) (expressed in the Healpix frame)
    * @throws Exception
    */
   public double [] pix2ang(int order,long npix) throws Exception {
      Pointing res = healpixBase[order].pix2ang(npix);
      return new double[]{ res.phi*180./Math.PI, (Math.PI/2. - res.theta)*180./Math.PI};
   }
   
   /** Provide the list of HEALPix numbers fully covering a circle (for a specified order)
    * @param order Healpix order
    * @param lon    center longitude (expressed in the Healpix frame)
    * @param lat    center latitude (expressed in the Healpix frame)
    * @param radius circle radius (in degrees)
    * @return
    * @throws Exception
    */
   public long [] queryDisc(int order, double lon, double lat, double radius) throws Exception {
      double theta = Math.PI/2. - lat/180.*Math.PI;
      double phi = lon/180.*Math.PI;
      RangeSet list = healpixBase[order].queryDisc(new Pointing(theta, phi), Math.toRadians(radius));
      if( list==null ) return new long[0];
      return list.toArray();
   }

   /*********************** private stuff ***************************************************/
   
   /** Maximal HEALPix order supported by the library */
   static public final int MAXORDER = 29;
   
   static private HealpixBase [] healpixBase;
   static {
      healpixBase = new HealpixBase[MAXORDER+1];
      try {
         for( int order=0; order<healpixBase.length; order++ ) {
            healpixBase[order] = new HealpixBase(pow2(order),Scheme.NESTED );
         }
      } catch( Exception e) { healpixBase=null; }
   }
   
   static public final long pow2(long order){ return 1<<order;}
   static public final long log2(long nside){ int i=0; while((nside>>>(++i))>0); return --i; }
   

}
