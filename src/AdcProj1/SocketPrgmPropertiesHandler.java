package AdcProj1;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class SocketPrgmPropertiesHandler {
	
	private final Properties configProp = new Properties();
	
	private SocketPrgmPropertiesHandler()
	   {
	      InputStream in = this.getClass().getClassLoader().getResourceAsStream("SocketProgramming.properties");
	      System.out.println("Read all properties from file");
	      try {
	          configProp.load(in);
	      } catch (IOException e) {
	          e.printStackTrace();
	      }
	   }
	   private static class InstanceKeeper
	   {
	      private static final SocketPrgmPropertiesHandler INSTANCE = new SocketPrgmPropertiesHandler();
	   }
	 
	   public static SocketPrgmPropertiesHandler getInstance()
	   {
	      return InstanceKeeper.INSTANCE;
	   }
	    
	   public String getProperty(String key){
		   System.out.println("returning key: "+key+ "and value: " +configProp.getProperty(key));
	      return configProp.getProperty(key);
	   }
	    
	   public Set<String> getAllPropertyNames(){
	      return configProp.stringPropertyNames();
	   }
	    
	   public boolean containsKey(String key){
	      return configProp.containsKey(key);
	   }
	}

