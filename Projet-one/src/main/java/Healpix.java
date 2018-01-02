import healpix.essentials.*;

public final class Healpix implements HealpixImpl {
   
  
   public long ang2pix(int order,double lon, double lat) throws Exception {
      double theta = Math.PI/2. - lat/180.*Math.PI;
      double phi = lon/180.*Math.PI;
      return healpixBase[order].ang2pix(new Pointing(theta,phi));
   }
   
  
   public double [] pix2ang(int order,long npix) throws Exception {
      Pointing res = healpixBase[order].pix2ang(npix);
      return new double[]{ res.phi*180./Math.PI, (Math.PI/2. - res.theta)*180./Math.PI};
   }
   
  
   public long [] queryDisc(int order, double lon, double lat, double radius) throws Exception {
      double theta = Math.PI/2. - lat/180.*Math.PI;
      double phi = lon/180.*Math.PI;
      RangeSet list = healpixBase[order].queryDisc(new Pointing(theta, phi), Math.toRadians(radius));
      if( list==null ) return new long[0];
      return list.toArray();
   }

  
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
