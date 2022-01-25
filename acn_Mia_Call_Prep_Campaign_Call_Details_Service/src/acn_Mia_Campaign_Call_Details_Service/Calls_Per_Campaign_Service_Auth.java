package acn_Mia_Campaign_Call_Details_Service;


import java.util.Base64;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Calls_Per_Campaign_Service_Auth {
	private static final Logger logger = LoggerFactory.getLogger(Calls_Per_Campaign_Service_Auth.class);
	
	String responseString;
	JSONObject jsonResultObject = new JSONObject();
	JSONArray jsonResultArray = new JSONArray();
	static WebAppParams params = null;
	static String ServiceAuth = null;

	static {
		params = new WebAppParams();
		ServiceAuth = (String) params.properties.get("ServiceAuth");
	}

	public boolean isUserAuthenticated(String authString) throws JSONException {
		if (authString == null) {
			authString = "Basic NoAuthentication";
			logger.debug("No authentication attempted.");
		}
		;

		String[] authParts = authString.split("\\s+");
		String authInfo = authParts[1];

		byte[] base64decodedBytes = Base64.getDecoder().decode(authInfo);
		String decodedAuthInfo = new String(base64decodedBytes);
		if (decodedAuthInfo.equals(ServiceAuth)) {
			logger.debug("Service_User_Authenticated: " + true);
			return true;
		} else {
			jsonResultObject.put("Error", "User not authenticated");
			jsonResultArray.put(jsonResultObject);
			this.responseString = jsonResultArray.toString();
		}
		return false;
	}

	public String getResponseString() {
		return this.responseString;
	}
}