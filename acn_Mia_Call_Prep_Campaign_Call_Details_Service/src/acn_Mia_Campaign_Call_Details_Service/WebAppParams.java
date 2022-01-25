package acn_Mia_Campaign_Call_Details_Service;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebAppParams {
	private static final Logger logger = LoggerFactory.getLogger(WebAppParams.class);
	final static String Properties_File = "service.properties";
	static Properties properties = null;
	
	static {

		// Call Prep Services Properties
		try {
			properties = new Properties();

			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			InputStream inputStream = classLoader.getResourceAsStream(Properties_File);

			InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF8");
			BufferedReader bufferedFileReader = new BufferedReader(inputStreamReader);

			properties.load(bufferedFileReader);

			Iterator callPrepPropsIterator = properties.entrySet().iterator();

			while (callPrepPropsIterator.hasNext()) {

				Map.Entry entry = (Map.Entry) callPrepPropsIterator.next();

				System.out.println("key: " + entry.getKey() + "    value: " + entry.getValue());
			}

			
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

}
