package acn_Mia_Campaign_Call_Details_Service;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.franz.agraph.repository.AGRepositoryConnection;

@Path("call-details")
public class Calls_Per_Campaign_Service {
	private static final Logger logger = LoggerFactory.getLogger(Calls_Per_Campaign_Service.class);

	static WebAppParams params = null;
	static String GraphHost = null;
	static String GraphDatabase = null;
	static String GraphCatalog = null;
	static String GraphUser = null;
	static String GraphPassword = null;
	static {
		params = new WebAppParams();
		GraphHost = (String) params.properties.get("GraphHost");
		GraphDatabase = (String) params.properties.get("GraphDatabase");
		GraphCatalog = (String) params.properties.get("GraphCatalog");
		GraphUser = (String) params.properties.get("GraphUser");
		GraphPassword = (String) params.properties.get("GraphPassword");

	}

	@Path("campaign-id/{campaignId}/start-date/{startDate}/end-date/{endDate}")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response relatedProducts(@PathParam("campaignId") String campaignId, @PathParam("startDate") String startDate, @PathParam("endDate") String endDate,
			@HeaderParam("Authorization") String authString) throws JSONException {

		// Authentication
		Calls_Per_Campaign_Service_Auth MyAuth = new Calls_Per_Campaign_Service_Auth();
		if (!MyAuth.isUserAuthenticated(authString)) {
			return Response.status(401).entity(MyAuth.getResponseString()).build();
		}

		logger.debug("Call Details Per Campaign --" + "  campaign-id: " + campaignId + "  start-date: " + startDate + "  end-date: " + endDate);

		Date beginDateDate;
		Date endDateDate;
		long difference = 0;
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			beginDateDate = dateFormat.parse(startDate);
			endDateDate = dateFormat.parse(endDate);

			difference = (long) ((endDateDate.getTime() - beginDateDate.getTime()) / (24 * 60 * 60 * 1000) % 365);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			StackTraceElement[] trace = e.getStackTrace();
			for (StackTraceElement element : trace) {
				logger.debug(element.toString());
			}
			logger.debug("difference: "+ difference);
			return Response.status(400).entity("Date Parsing Error.").build();
		}

		if (difference > 32) {
			logger.debug("Maximum date difference is 32 days.");
			return Response.status(400).entity("Maximum date difference is 32 days.").build();

		}

		// Initialize JSON Array for response to REST Service
		JSONArray jsonResultArray = new JSONArray();

		// Database Connection
		AGRepositoryConnection conn = DatabaseOps.agOpenConn(GraphHost, GraphCatalog, GraphDatabase, GraphUser, GraphPassword);

		// Query String
//		String queryString = "# Campaign Call Details\n" 
//		+ "#select ?agentFirstName ?agentLastName (count(distinct ?callId) as ?callCount)\n"
//		+ "select distinct ?callId ?campaignId ?customerName ?campaignName ?employeeId ?agentFirstName ?agentLastName \n" 
//		+ "?duration ?direction ?result \n"
//		+ "?caStartDate ?callDate ?caEndDate ?callStartTime ?fromPhone ?toPhone \n" 
//		+ "where{\n" 
//		+ "  {select ?campaignId ?campaignName ?customerName\n" 
//		+ "where { \n"
//		+ "  graph <http://n3results.com/hub/graph#campaigns> {\n" 
//		+ "   ?campaignId <http://n3results.com/hub/campaign/pred#name> ?campaignName .\n"
//		+ "      ?campaignId <http://n3results.com/hub/campaign/pred#customerName> ?customerName .\n" 
//		+ "  filter(?campaignId= <http://n3results.com/hub/campaign/campaignId#"+ campaignId +">) .\n"
//		+ " }}}\n" 
//		+ "  \n" + "{ select distinct ?employeeId ?campaignId ?caEndDate ?caStartDate    \n" 
//		+ "where {graph  <http://n3results.com/hub/graph#campaignAssignment> {\n"
//		+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#employeeId> ?employeeId .\n" 
//		+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#roleIdName> ?campaignRoleIdName .\n"
//		+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#endDate> ?caEndDate .\n" 
//		+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#startDate> ?caStartDate .\n"
//		+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#campaignId> ?campaignId . \n" 
//		+ "  filter (?campaignRoleIdName = \"BDR\"  \n"
//		+ "		   || ?campaignRoleIdName = \"IOM\" \n" 
//		+ "		   || ?campaignRoleIdName = \"Sr. IOM\" \n" 
//		+ "		   || ?campaignRoleIdName = \"Qualifier\" \n"
//		+ "		   || ?campaignRoleIdName = \"L400\"  \n" 
//		+ "		   || ?campaignRoleIdName = \"LSS\") .\n" 
//		+ "  }}}\n" 
//		+ "  \n"
//		+ "  {select distinct ?employeeId ?extId ?agentFirstName ?agentLastName ?email\n" 
//		+ "where{  graph <http://n3results.com/hub/graph#employees> {\n"
//		+ "   ?employeeId <http://n3results.com/hub/employee/pred#extensionId> ?extId .\n" 
//		+ "   ?employeeId <http://n3results.com/hub/employee/pred#firstName> ?agentFirstName .\n"
//		+ "   ?employeeId <http://n3results.com/hub/employee/pred#lastName> ?agentLastName .\n" 
//		+ "   ?employeeId <http://n3results.com/hub/employee/pred#email> ?email .\n" 
//		+ "}}}\n" 
//		+ "  \n"
//		+ "{select distinct ?callId  ?callDate ?callStartTime ?duration ?direction \n" 
//		+ "  ?fromPhone ?toPhone ?extId ?result  \n"
//		+ "where { graph <http://n3results.com/rc/graph#calls> {  \n" 
//		+ "?callId <http://n3results.com/rc/call/pred#direction> ?direction .\n"
//		+ "?callId <http://n3results.com/rc/call/pred#extensionId> ?extId .\n" 
//		+ "?callId <http://n3results.com/rc/call/pred#duration> ?duration . \n"
//		+ "?callId <http://n3results.com/rc/call/pred#startTime> ?callStartTime .\n" 
//		+ "?callId <http://n3results.com/rc/call/pred#toPhoneNumber> ?toPhone . \n"
//		+ "?callId <http://n3results.com/rc/call/pred#result> ?result . \n" 
//		+ "  bind(xsd:date(?callStartTime) as ?callDate) . \n" 
//		+ "  filter(?callDate >= \"" + startDate + "Z\"^^xsd:date) .\n"
//		+ "  filter(?callDate <= \"" + endDate + "Z\"^^xsd:date) .\n" 
//		+ "}}}\n" + "\n"
//		+ "filter(?callDate >= ?caStartDate && ?callDate <= ?caEndDate ) .\n" 
//		+ "}\n" 
//		+ "#group by ?agentFirstName ?agentLastName\n" 
//		+ "#order by desc(?duration)\n" 
//		+ "\n";
		
		String queryString = "select distinct ?callId  \n"
				+ "?campaignId ?customerName ?campaignName ?employeeId ?agentFirstName ?agentLastName \n"
				+ "?duration ?direction ?result \n"
				+ "#?caStartDate ?caEndDate \n"
				+ " ?callStartTime #?caStartDateTime ?caEndDateTime"
				+ "?fromPhone ?toPhone \n"
				+ "\n"
				+ "where{\n"
				+ "  {select ?campaignId \n"
				+ "   ?campaignName ?customerName\n"
				+ "where { \n"
				+ "  graph <http://n3results.com/hub/graph#campaigns> {\n"
				+ "   ?campaignId <http://n3results.com/hub/campaign/pred#name> ?campaignName .\n"
				+ "      ?campaignId <http://n3results.com/hub/campaign/pred#customerName> ?customerName .\n"
				+ "  filter(?campaignId= <http://n3results.com/hub/campaign/campaignId#" + campaignId + ">) .\n"
				+ " }}}\n"
				+ "  \n"
				+ "{ select distinct ?employeeId ?campaignId ?caEndDate ?caStartDate ?caStartDateTime ?caEndDateTime    \n"
				+ "where {graph  <http://n3results.com/hub/graph#campaignAssignment> {\n"
				+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#employeeId> ?employeeId .\n"
				+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#roleIdName> ?campaignRoleIdName .\n"
				+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#endDate> ?caEndDate .\n"
				+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#startDate> ?caStartDate .\n"
				+ "  ?ca <http://n3results.com/hub/campaignAssignment/pred#campaignId> ?campaignId .\n"
				+ "  filter(?campaignId = <http://n3results.com/hub/campaign/campaignId#" + campaignId + ">).\n"
				+ " filter (?campaignRoleIdName = \"BDR\"  \n"
				+ "		   || ?campaignRoleIdName = \"IOM\" \n"
				+ "		   || ?campaignRoleIdName = \"Qualifier\" \n"
				+ "		   || ?campaignRoleIdName = \"L400\"  \n"
				+ "		   || ?campaignRoleIdName = \"LSS\") .\n"
				+ " \n"
				+ " # <http://n3results.com/rc/call/startDateKeyUtc#2021-11-08>\n"
				+ "  }\n"
				+ " bind(xsd:dateTime(?caEndDate) as ?caEndDateTime)\n"
				+ " bind(xsd:dateTime(?caStartDate) as ?caStartDateTime).\n"
				+ "  }}\n"
				+ "  \n"
				+ "  {select distinct ?employeeId ?extId ?agentFirstName ?agentLastName ?email\n"
				+ "where{  graph <http://n3results.com/hub/graph#employees> {\n"
				+ "   ?employeeId <http://n3results.com/hub/employee/pred#extensionId> ?extId .\n"
				+ "   ?employeeId <http://n3results.com/hub/employee/pred#firstName> ?agentFirstName .\n"
				+ "   ?employeeId <http://n3results.com/hub/employee/pred#lastName> ?agentLastName .\n"
				+ "   ?employeeId <http://n3results.com/hub/employee/pred#email> ?email .\n"
				+ "}}}\n"
				+ "  graph <http://n3results.com/rc/graph#calls> {   \n"
				+ " ?callId <http://n3results.com/rc/call/pred#extensionId> ?extId .\n"
				+ "  ?callId <http://n3results.com/rc/call/pred#startTime>  ?callStartTime .\n"
				+ "  filter(?callStartTime > \"" + startDate + "T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )\n"
				+ "  filter(?callStartTime < \"" + endDate + "T23:59:59.9999Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )\n"
				+ " }\n"
				+ " filter(?caStartDateTime<=?callStartTime && \n"
				+ "        ?caEndDateTime >= ?callStartTime). \n"
				+ "\n"
				+ "{select ?callId ?direction ?curation ?toPhone ?fromPhone ?result ?duration\n"
				+ " where{ graph  <http://n3results.com/rc/graph#calls>  {\n"
				+ "?callId <http://n3results.com/rc/call/pred#direction> ?direction .\n"
				+ "?callId <http://n3results.com/rc/call/pred#duration> ?duration . \n"
				+ "?callId <http://n3results.com/rc/call/pred#toPhoneNumber> ?toPhone . \n"
				+ "?callId <http://n3results.com/rc/call/pred#fromPhoneNumber> ?fromPhone . \n"
				+ "?callId <http://n3results.com/rc/call/pred#result> ?result . \n"
				+ "}}} "
				+ "}";		


		System.out.print(queryString);
		logger.debug(queryString);

		// Prepare Query
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result;

		// Execute Query and Bind Results to jsonResultsArray
		try {
			result = tupleQuery.evaluate();
			logger.debug("Started Processing Query");
			while (result.hasNext()) {
				JSONObject jsonResultRow = new JSONObject();
				BindingSet bindingSet = result.next();

				// ?callId
				jsonResultRow.put("callId", bindingSet.getBinding("callId").getValue().stringValue().replaceAll("(^.*#)(.*$)", "$2"));

				// ?campaignId
				jsonResultRow.put("campaignId", bindingSet.getBinding("campaignId").getValue().stringValue().replaceAll("(^.*#)(.*$)", "$2"));

				// ?customerName
				jsonResultRow.put("customerName", bindingSet.getBinding("customerName").getValue().stringValue());

				// ?campaignName
				jsonResultRow.put("campaignName", bindingSet.getBinding("campaignName").getValue().stringValue());

				// ?employeeId
				jsonResultRow.put("employeeId", bindingSet.getBinding("employeeId").getValue().stringValue().replaceAll("(^.*#)(.*$)", "$2"));

				// ?agentFirstName
				jsonResultRow.put("agentFirstName", bindingSet.getBinding("agentFirstName").getValue().stringValue());

				// ?agentLastName
				jsonResultRow.put("agentLastName", bindingSet.getBinding("agentLastName").getValue().stringValue());

				// ?duration
				jsonResultRow.put("duration", bindingSet.getBinding("duration").getValue().stringValue());

				// ?direction
				jsonResultRow.put("direction", bindingSet.getBinding("direction").getValue().stringValue().replaceAll("(^.*#)(.*$)", "$2"));

				// ?result
				jsonResultRow.put("result", bindingSet.getBinding("result").getValue().stringValue().replaceAll("(^.*#)(.*$)", "$2"));

//				// ?callDate
//				jsonResultRow.put("callDate", bindingSet.getBinding("callDate").getValue().stringValue().replaceAll( "(^.*)(Z$)", "$1"));

				// ?callStartTime
				jsonResultRow.put("callStartTime", bindingSet.getBinding("callStartTime").getValue().stringValue());

				// ?fromPhone
				if(bindingSet.getBinding("fromPhone")!=null) {
				jsonResultRow.put("fromPhone", bindingSet.getBinding("fromPhone").getValue().stringValue().replaceAll("(^.*#)(.*$)", "$2"));
				}
				// ?toPhone
				if(bindingSet.getBinding("toPhone")!= null) {
				jsonResultRow.put("toPhone", bindingSet.getBinding("toPhone").getValue().stringValue().replaceAll("(^.*#)(.*$)", "$2"));
				}
				
				jsonResultArray.put(jsonResultRow);
			}
			result.close();

		} catch (Exception e) {
			e.printStackTrace();
			StackTraceElement[] trace = e.getStackTrace();
			for (StackTraceElement element : trace) {
				logger.debug(element.toString());
			}

		} finally {
			DatabaseOps.agCloseConn(conn);

		}

		JSONObject jsonResult = new JSONObject();
		jsonResult.put("resultSet", jsonResultArray);

		String jsonResultString = jsonResultArray.toString();
		logger.debug("Campaign Call Details: " + campaignId + "  startDate: " + startDate + "  endDate: " + endDate);
		logger.debug("First 500 Characters of results:" 
				+ jsonResultString.substring(0, jsonResultString.length() < 500 ? jsonResultString.length()-1 : 499));

		return Response.status(200).entity(jsonResult.toString()).build();
	}
}
