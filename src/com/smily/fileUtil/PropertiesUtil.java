package com.smily.fileUtil;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertiesUtil {
	
	private static String runtimeProp = "./config/runtime.properties";
	
	private static Logger log 	= Logger.getLogger(PropertiesUtil.class.getName());
	
	public static int srcDb_type;
	public static String srcDb_driver;
	public static String srcDb_link;
	public static String srcDb_user;
	public static String srcDb_password;
	public static String srcDb_table;
	
	public static int destDb_type;
	public static String destDb_driver;
	public static String destDb_link;
	public static String destDb_user;
	public static String destDb_password;
	public static String destDb_table;
	
	public static String primaryKey;
	public static int recordPerBatch = 500;
	public static int pkStartValue = -1;
	
	public static String srcDb_database;
	public static String destDb_database;
	
	public static void getProperties(){

		Properties prop = new Properties();     
		try{
			//读取属性文件runtime.properties
			InputStream in = new BufferedInputStream( new FileInputStream(runtimeProp) );
			prop.load(in);//加载属性列表
			PropertiesUtil.srcDb_type 		= Integer.parseInt( prop.getProperty( "srcDb.type" ) );
			PropertiesUtil.srcDb_driver	  	= prop.getProperty( "srcDb.driver" ) ;
			PropertiesUtil.srcDb_link  	  	= prop.getProperty( "srcDb.link" );			
			PropertiesUtil.srcDb_user    	= prop.getProperty( "srcDb.user" );
			PropertiesUtil.srcDb_password   = prop.getProperty( "srcDb.password" );
			PropertiesUtil.srcDb_table		= prop.getProperty( "srcDb.table" );
			PropertiesUtil.destDb_type 		= Integer.parseInt( prop.getProperty( "destDb.type" ) );
			PropertiesUtil.destDb_driver	= prop.getProperty( "destDb.driver" ) ;
			PropertiesUtil.destDb_link  	= prop.getProperty( "destDb.link" );			
			PropertiesUtil.destDb_user    	= prop.getProperty( "destDb.user" );
			PropertiesUtil.destDb_password  = prop.getProperty( "destDb.password" );
			PropertiesUtil.destDb_table		= prop.getProperty( "destDb.table" );	
			PropertiesUtil.primaryKey		= prop.getProperty( "primaryKey" );
			PropertiesUtil.recordPerBatch	= Integer.parseInt( prop.getProperty( "recordPerBatch" ) );
			PropertiesUtil.pkStartValue		= Integer.parseInt( prop.getProperty( "pkStartValue" ) );
			
			PropertiesUtil.srcDb_database	= PropertiesUtil.srcDb_link.substring( PropertiesUtil.srcDb_link.lastIndexOf("/")+1 );
			if( PropertiesUtil.srcDb_database.indexOf("?")>-1 ){
				PropertiesUtil.srcDb_database = PropertiesUtil.srcDb_database.substring(0,PropertiesUtil.srcDb_database.indexOf("?"));
			}
			PropertiesUtil.destDb_database	= PropertiesUtil.srcDb_link.substring( PropertiesUtil.destDb_link.lastIndexOf("/")+1 );
			if( PropertiesUtil.destDb_database.indexOf("?")>-1 ){
				PropertiesUtil.destDb_database = PropertiesUtil.destDb_database.substring(0,PropertiesUtil.destDb_database.indexOf("?"));
			}
			
			in.close();	
			log.info( "初始化成功：" 
					+ "\r\n" + "srcDb_type=" + PropertiesUtil.srcDb_type
					+ "\r\n" + "srcDb_link=" + PropertiesUtil.srcDb_link 
					+ "\r\n" + "srcDb_table=" + PropertiesUtil.srcDb_table 
					+ "\r\n" + "destDb_type=" + PropertiesUtil.destDb_type
					+ "\r\n" + "destDb_link=" + PropertiesUtil.destDb_link
					+ "\r\n" + "destDb_table=" + PropertiesUtil.destDb_table
					+ "\r\n" + "primaryKey=" + PropertiesUtil.primaryKey 
					+ "\r\n" + "recordPerBatch=" + PropertiesUtil.recordPerBatch
					+ "\r\n" + "pkStartValue=" + PropertiesUtil.pkStartValue
					);
		}
		catch(Exception e){
			log.error( e.getMessage() );
		}
	
	}

	public static void updateProperties( String key, String value ){
		Properties prop = new Properties();// 属性集合对象 
		FileInputStream fis;
		try {
			fis = new FileInputStream(runtimeProp);
			prop.load(fis);// 将属性文件流装载到Properties对象中 
			fis.close();// 关闭流
		} catch (FileNotFoundException e) {
			log.error( e.getMessage() );
		} catch (IOException e) {
			log.error( e.getMessage() );
		}
		prop.setProperty(key, value); 
		// 文件输出流 
		try {
			FileOutputStream fos = new FileOutputStream(runtimeProp); 
			// 将Properties集合保存到流中 
			prop.store(fos, "Copyright (c) Boxcode Studio"); 
			fos.close();// 关闭流 
		} catch (FileNotFoundException e) {
			log.error( e.getMessage() );
		} catch (IOException e) {
			log.error( e.getMessage() );
		}
		log.info( "获取修改后的属性值：pkStartValue=" + prop.getProperty("pkStartValue") ); 		
	}
	
}
