package com.partition.file.partition;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import healpix.essentials.*;
public class Partition {

	public static void main(String[] args){
		
		String chaine="";
		String fichier ="/home/vaadl/Bureau/DDI";
		
		//lecture du fichier	
		try{
			InputStream ips=new FileInputStream(fichier); 
			InputStreamReader ipsr=new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			String ligne;
			while ((ligne=br.readLine())!=null){
				System.out.println(ligne);
				chaine+=ligne+"\n";
			}
			br.close(); 
		}		
		catch (Exception e){
			System.out.println(e.toString());
		}
		
		//Partition du fichier
		System.out.printf("Partitionnement healpix");
        int nSide = 16;
        
        try {
        	HealpixBase healpixBase = new HealpixBase(nSide, Scheme.NESTED);
        	Double phi = Math.PI;
            Double theta = Math.PI / 2;
            
           // chaine == healpixBase.ang2pix(new Pointing(theta, phi));
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}
}
