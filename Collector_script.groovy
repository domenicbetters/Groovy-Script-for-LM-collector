import groovy.sql.Sql
import com.santaba.agent.groovyapi.http.*;
import groovy.json.JsonSlurper;
import org.apache.http.HttpEntity
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPut
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import groovy.json.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.utils.URIBuilder








// ***  Establish API Creds  ***
def accessId = hostProps.get("lmaccess.id");
def accessKey = hostProps.get("lmaccess.key");
def account = 'micoresolutions';


// *** Get Datasource ID for device as string. ***
def resourcePath = '/device/devices/'+hostProps.get("system.deviceID")+'/devicedatasources'
def url = "https://" + account + ".logicmonitor.com" + "/santaba/rest" + resourcePath;

    //get current time
epoch = System.currentTimeMillis();

    //calculate signature
requestVars = "GET" + epoch + resourcePath;

hmac = Mac.getInstance("HmacSHA256");
secret = new SecretKeySpec(accessKey.getBytes(), "HmacSHA256");
hmac.init(secret);
hmac_signed = Hex.encodeHexString(hmac.doFinal(requestVars.getBytes()));
signature = hmac_signed.bytes.encodeBase64();

    // HTTP GET
CloseableHttpClient httpdsid = HttpClients.createDefault();
httpGet = new HttpGet(url);
    // Add custom filter values to GET Request 
URI uri = new URIBuilder(httpGet.getURI())
      .addParameter("fields", "id")
      .addParameter("filter", "dataSourceName:Microsoft_SQLServer_GlobalPerformance")
      .build();
      httpGet.setURI(uri);

httpGet.addHeader("Authorization" , "LMv1 " + accessId + ":" + signature + ":" + epoch);
response = httpdsid.execute(httpGet);
responseBody = EntityUtils.toString(response.getEntity());
    // Turn data into id string that can be inserted into next Request 
datapull = new JsonSlurper().parseText(responseBody.toString());
datapull =   datapull.data.items.id.value.toString()
datapull = datapull.replace("[","")
datasouceid = datapull.replace("]","")

httpdsid.close()

//  *** get instance ID for each instance on device ***

 resourcePath = "/device/devices/"+hostProps.get("system.deviceID")+"/devicedatasources/${datasouceid}/instances"
 url = "https://" + account + ".logicmonitor.com" + "/santaba/rest" + resourcePath;

    //get current time
epoch = System.currentTimeMillis();

    //calculate signature
requestVars = "GET" + epoch + resourcePath;

hmac = Mac.getInstance("HmacSHA256");
secret = new SecretKeySpec(accessKey.getBytes(), "HmacSHA256");
hmac.init(secret);
hmac_signed = Hex.encodeHexString(hmac.doFinal(requestVars.getBytes()));
signature = hmac_signed.bytes.encodeBase64();

    // HTTP GET
CloseableHttpClient httpinstanceid = HttpClients.createDefault();
httpGet = new HttpGet(url);
httpGet.addHeader("Authorization" , "LMv1 " + accessId + ":" + signature + ":" + epoch);
response = httpinstanceid.execute(httpGet);
responseBody = EntityUtils.toString(response.getEntity());
    // Male array of instance ids for final get and put request 
datapull = new JsonSlurper().parseText(responseBody.toString());
instanceids =   datapull.data.items.id

httpinstanceid.close()





// ***Pulls the instance names from the host***
def sqlInstances = hostProps.get("mssql.sql_server_instances") ?: hostProps.get("auto.sql_server_instances") // override sql instances with manual instances
Boolean debug = false

LMDebugPrint("***** Running GlobalPerformance", debug)

    //the map of counters retrieved and the where clause are used to limit the number of records returned.
Map countersRetrieved = 
['ActiveTempTables' : 'Active Temp Tables',
'Backgroundwriterpages/sec' : 'Background writer pages/sec',
'BatchRequests/sec' : 'Batch Requests/sec',
'Buffercachehitratio' : 'Buffer cache hit ratio',
'Buffercachehitratiobase' : 'Buffer cache hit ratio base',
'Checkpointpages/sec' : 'Checkpoint pages/sec',
'ConnectionMemory(bytes)' : 'Connection Memory (KB)',
'ConnectionReset/sec' : 'Connection Reset/sec',
'DatabaseCacheMemory(bytes)' : 'Database Cache Memory (KB)',
'Databasepages' : 'Database pages',
'FreeMemory(bytes)' : 'Free Memory (KB)',
'FullScans/sec' : 'Full Scans/sec',
'GrantedWorkspaceMemory(bytes)': 'Granted Workspace Memory (KB)',
'IndexSearches/sec' : 'Index Searches/sec',
'LatchWaits/sec' : 'Latch Waits/sec',
'Lazywrites/sec' : 'Lazy writes/sec',
'LockBlocks' : 'Lock Blocks',
'LockBlocksAllocated' : 'Lock Blocks Allocated',
'LockMemory(bytes)' : 'Lock Memory (KB)',
'LockOwnerBlocks' : 'Lock Owner Blocks',
'LockOwnerBlocksAllocated' : 'Lock Owner Blocks Allocated',
'LockRequests/sec' : 'Lock Requests/sec',
'LockTimeouts/sec' : 'Lock Timeouts/sec',
'LockWaits/sec' : 'Lock Waits/sec',
'LockWaitTime(ms)' : 'Lock Wait Time (ms)',
'LogicalConnections' : 'Logical Connections',
'Logins/sec' : 'Logins/sec',
'Logouts/sec' : 'Logouts/sec',
'LogPoolMemory(bytes)' : 'Log Pool Memory (KB)',
'MaximumWorkspaceMemory(bytes)': 'Maximum Workspace Memory (KB)',
'MemoryGrantsOutstanding' : 'Memory Grants Outstanding',
'MemoryGrantsPending' : 'Memory Grants Pending',
'NumberofDeadlocks/sec' : 'Number of Deadlocks/sec',
'NumberofSuperLatches' : 'Number of SuperLatches',
'OpenConnectionCount' : 'Open Connection Count',
'OptimizerMemory(bytes)' : 'Optimizer Memory (KB)',
'PageDeallocations/sec' : 'Page Deallocations/sec',
'Pagelifeexpectancy' : 'Page life expectancy',
'Pagelookups/sec' : 'Page lookups/sec',
'Pagereads/sec' : 'Page reads/sec',
'PagesAllocated/sec' : 'Pages Allocated/sec',
'PageSplits/sec' : 'Page Splits/sec',
'Pagewrites/sec' : 'Page writes/sec',
'Processesblocked' : 'Processes blocked',
'Readaheadpages/sec' : 'Readahead pages/sec',
'Readaheadtime/sec' : 'Readahead time/sec',
'ReceiveI/Obytes/sec' : 'Receive I/O bytes/sec',
'ReceiveI/OLenAvg' : 'Receive I/O Len Avg',
'ReceiveI/Os/sec' : 'Receive I/Os/sec',
'ReservedServerMemory(bytes)' : 'Reserved Server Memory (KB)',
'SendI/Obytes/sec' : 'Send I/O bytes/sec',
'SendI/OLenAvg' : 'Send I/O Len Avg',
'SendI/Os/sec' : 'Send I/Os/sec',
'SQLCacheMemory(bytes)' : 'SQL Cache Memory (KB)',
'SQLCompilations/sec' : 'SQL Compilations/sec',
'SQLRe-Compilations/sec' : 'SQL Re-Compilations/sec',
'StolenServerMemory(bytes)' : 'Stolen Server Memory (KB)',
'SuperLatchDemotions/sec' : 'SuperLatch Demotions/sec',
'SuperLatchPromotions/sec' : 'SuperLatch Promotions/sec',
'Targetpages' : 'Target pages',
'TargetServerMemory(bytes)' : 'Target Server Memory (KB)',
'TotalLatchWaitTime(ms)' : 'Total Latch Wait Time (ms)',
'TotalServerMemory(bytes)' : 'Total Server Memory (KB)',
'Transactions/sec' : 'Transactions/sec',
'UserConnections' : 'User Connections',
'WorkfilesCreated/sec' : 'Workfiles Created/sec',
'WorktablesCreated/sec' : 'Worktables Created/sec'
]

// Get all of the database names to use for the where clause

def whereClause = ''
countersRetrieved.each { 
    intFormat, sqlFormat ->
    whereClause += "'${sqlFormat}',"
}
//add nothing to the where clause to make it all work
whereClause += "'nothingatall')"

def perf_counter_query = """
SELECT
CASE
WHEN CHARINDEX('KB', counter_name) > 0 THEN cntr_value * 1024
ELSE cntr_value
END AS cntr_value,
REPLACE(REPLACE(counter_name, ' ', ''), '(KB)', '(bytes)') AS counter_names
FROM sys.dm_os_performance_counters
WHERE (instance_name = '_Total'
OR instance_Name = ' ')
AND counter_name IN (""" + whereClause + " order by counter_name"
LMDebugPrint("\tSQL Instances ${sqlInstances}", debug)
LMDebugPrint("Perf Counter Query: ${perf_counter_query}", debug)
if (sqlInstances)
{
// ***Using SQL query to iterate through each instance's datapoints***
    sqlInstances.toString().tokenize(",").each
    { instanceName ->
        instanceName= instanceName.replaceAll(/[:|\\|\s|=]+/, "_")
        def blockerid = "No Blocks"
        LMDebugPrint("\tInstance Name ${instanceName}", debug)
        def jdbcConnectionString = hostProps.get("mssql.${instanceName.trim()}.mssql_url") ?: hostProps.get("auto.${instanceName.trim()}.mssql_url")
        def user = hostProps.get("mssql.${instanceName.trim()}.user") ?: hostProps.get("jdbc.mssql.user") ?: ""
        def pass = hostProps.get("mssql.${instanceName.trim()}.pass") ?: hostProps.get("jdbc.mssql.pass") ?: ""
        LMDebugPrint("\tJDBC Connection String ${jdbcConnectionString}", debug)
        // ensure we dont have any null's
        if (jdbcConnectionString)
        {
            def conn = attemptConnection(user, pass, jdbcConnectionString, debug)
            if (conn.status == "success")
            {

                outputRecords = runQuery(perf_counter_query, conn.connection, debug)
                if (outputRecords.status == 'success')
                {

                    outputRecords.data.each { oneRecord ->
                        def counter_name = oneRecord.counter_names.toString().replaceAll(" ", "")
                        def cntr_value = oneRecord.cntr_value
                        println "${instanceName}.${counter_name}=${cntr_value}"
// ***if statement to run final request only once, during the processblocked iteration***
                        if ("${counter_name}"=="Processesblocked")
                        {
// ***Follow up if statement that decides between which Put request to use based on whether there is in fact a block or not. ***
                            if ("${cntr_value}".toInteger() > 0)
                            {
// ***For loop to cycle through each instance on the device pulled from instance array***
                                for(i=0; i < instanceids.size; i++) 
                                {

                                    resourcePath = "/device/devices/"+hostProps.get("system.deviceID")+"/devicedatasources/${datasouceid}/instances/"+instanceids[i]
                                    url = "https://" + account + ".logicmonitor.com" + "/santaba/rest" + resourcePath;

                                    //get current time
                                    epoch = System.currentTimeMillis();

                                    //calculate signature
                                    requestVars = "GET" + epoch + resourcePath;

                                    hmac = Mac.getInstance("HmacSHA256");
                                    secret = new SecretKeySpec(accessKey.getBytes(), "HmacSHA256");
                                    hmac.init(secret);
                                    hmac_signed = Hex.encodeHexString(hmac.doFinal(requestVars.getBytes()));
                                    signature = hmac_signed.bytes.encodeBase64();

                                    // HTTP GET
                                    CloseableHttpClient httpinprop = HttpClients.createDefault();
                                    httpGet = new HttpGet(url);
                                    httpGet.addHeader("Authorization" , "LMv1 " + accessId + ":" + signature + ":" + epoch);
                                    response = httpinprop.execute(httpGet);
                                    responseBody = EntityUtils.toString(response.getEntity());
                                    code = response.getStatusLine().getStatusCode();
                                    datapull = new JsonSlurper().parseText(responseBody.toString());
                                    instanceprops =   datapull.data
// ***if statement to ensure that the put request only applies to current SQL instance iteration by matching the instance displayname property with the SQL instancename ***
                                    if ("${instanceprops.displayName}"=="${instanceName}")
                                    {
                                       // ***SQL query to find blocker data***
                                        outputBlocks = runQuery("""SELECT SPID, BLOCKED, REPLACE (REPLACE (T.TEXT, CHAR(10), ' '), CHAR (13), ' ' ) AS BATCH INTO #T FROM sys.sysprocesses R CROSS APPLY sys.dm_exec_sql_text(R.SQL_HANDLE) T; WITH BLOCKERS (SPID, BLOCKED, LEVEL, BATCH) AS (SELECT SPID, BLOCKED, CAST (REPLICATE ('0', 4-LEN (CAST (SPID AS VARCHAR))) + CAST (SPID AS VARCHAR) AS VARCHAR (1000)) AS LEVEL, BATCH FROM #T R WHERE (BLOCKED = 0 OR BLOCKED = SPID) AND EXISTS (SELECT * FROM #T R2 WHERE R2.BLOCKED = R.SPID AND R2.BLOCKED <> R2.SPID) UNION ALL SELECT R.SPID, R.BLOCKED, CAST (BLOCKERS.LEVEL + RIGHT (CAST ((1000 + R.SPID) AS VARCHAR (100)), 4) AS VARCHAR (1000)) AS LEVEL, R.BATCH FROM #T AS R INNER JOIN BLOCKERS ON R.BLOCKED = BLOCKERS.SPID WHERE R.BLOCKED > 0 AND R.BLOCKED <> R.SPID ) SELECT N'    ' + REPLICATE (N'|         ', LEN (LEVEL)/4 - 1) + CASE WHEN (LEN(LEVEL)/4 - 1) = 0 THEN 'HEAD -  ' ELSE '|------  ' END + CAST (SPID AS NVARCHAR (10)) + N' ' + BATCH AS BLOCKING_TREE FROM BLOCKERS ORDER BY LEVEL ASC DROP TABLE #T;""", conn.connection, debug)

                                        blockerid = "${outputBlocks.data.blocking_tree}"

                                        instanceprops.customProperties = [[name:"blockerdetails", value:"${blockerid}"]]
                                        instanceprops = JsonOutput.toJson(instanceprops)
                                        StringEntity params = new StringEntity(instanceprops,ContentType.APPLICATION_JSON)

                                        epoch = System.currentTimeMillis(); //get current time

                                        requestVars = "PUT" + epoch + instanceprops + resourcePath;

                                        hmac = Mac.getInstance("HmacSHA256");
                                        secret = new SecretKeySpec(accessKey.getBytes(), "HmacSHA256");
                                        hmac.init(secret);
                                        hmac_signed = Hex.encodeHexString(hmac.doFinal(requestVars.getBytes()));
                                        signature = hmac_signed.bytes.encodeBase64();

                                        httpPut = new HttpPut(url);
                                        httpPut.addHeader("Authorization" , "LMv1 " + accessId + ":" + signature + ":" + epoch);
                                        httpPut.setHeader("Accept", "application/json");
                                        httpPut.setHeader("Content-type", "application/json");
                                        httpPut.setEntity(params);
                                        responsePut = httpinprop.execute(httpPut);
                                        responseBodyPut = EntityUtils.toString(responsePut.getEntity());
                                        codePut = responsePut.getStatusLine().getStatusCode();

                                    }
                                        

                                    httpinprop.close()
                                }

                            }
                            else 
                            {
                                for(i=0; i < instanceids.size; i++) 
                                {
                                    
                                    resourcePath = "/device/devices/"+hostProps.get("system.deviceID")+"/devicedatasources/${datasouceid}/instances/"+instanceids[i]
                                    url = "https://" + account + ".logicmonitor.com" + "/santaba/rest" + resourcePath;

                                    //get current time
                                    epoch = System.currentTimeMillis();

                                    //calculate signature
                                    requestVars = "GET" + epoch + resourcePath;

                                    hmac = Mac.getInstance("HmacSHA256");
                                    secret = new SecretKeySpec(accessKey.getBytes(), "HmacSHA256");
                                    hmac.init(secret);
                                    hmac_signed = Hex.encodeHexString(hmac.doFinal(requestVars.getBytes()));
                                    signature = hmac_signed.bytes.encodeBase64();

                                    // HTTP GET
                                    CloseableHttpClient httpinprop = HttpClients.createDefault();
                                    httpGet = new HttpGet(url);
                                    httpGet.addHeader("Authorization" , "LMv1 " + accessId + ":" + signature + ":" + epoch);
                                    response = httpinprop.execute(httpGet);
                                    responseBody = EntityUtils.toString(response.getEntity());
                                    code = response.getStatusLine().getStatusCode();
                                    datapull = new JsonSlurper().parseText(responseBody.toString());
                                    instanceprops =   datapull.data
// ***if statement to ensure that the put request only applies to current SQL instance iteration by matching the instance displayname property with the SQL instancename ***
                                    if ("${instanceprops.displayName}"=="${instanceName}")
                                    {
                                        blockerid = "No Blocks"

                                        instanceprops.customProperties = [[name:"blockerdetails", value:"${blockerid}"]]
                                        instanceprops = JsonOutput.toJson(instanceprops)
                                        StringEntity params = new StringEntity(instanceprops,ContentType.APPLICATION_JSON)

                                        epoch = System.currentTimeMillis(); //get current time

                                        requestVars = "PUT" + epoch + instanceprops + resourcePath;

                                        hmac = Mac.getInstance("HmacSHA256");
                                        secret = new SecretKeySpec(accessKey.getBytes(), "HmacSHA256");
                                        hmac.init(secret);
                                        hmac_signed = Hex.encodeHexString(hmac.doFinal(requestVars.getBytes()));
                                        signature = hmac_signed.bytes.encodeBase64();

                                        httpPut = new HttpPut(url);
                                        httpPut.addHeader("Authorization" , "LMv1 " + accessId + ":" + signature + ":" + epoch);
                                        httpPut.setHeader("Accept", "application/json");
                                        httpPut.setHeader("Content-type", "application/json");
                                        httpPut.setEntity(params);
                                        responsePut = httpinprop.execute(httpPut);
                                        responseBodyPut = EntityUtils.toString(responsePut.getEntity());
                                        codePut = responsePut.getStatusLine().getStatusCode();
                                    }
                                    httpinprop.close()
                                }
                            }
                        }
                    }
                }

                conn.connection.close()
            }
        }
    }
    return 0
}
else
{
    return 1
}


/**
* Helper method to print out debug messages for troubleshooting purposes.
* @param message
* @param debug
* @return
*/

def LMDebugPrint(message, Boolean debug = false)
{
    if (debug)
    {
        println(message.toString())
    }
}


/**
* Helper method which handles creating a connection to the jdbc database
* @returnArray is an array with a connection, status and any error messages an array.
* *connection = jdbc connection
* *status, success or fail
* *errors, if both connection types fail there will be 2 error messages.
*
*/
def attemptConnection(String instanceUser, String instancePass, String instanceURL, Boolean debug = false)
{
    LMDebugPrint("**** Props to connect:", debug)
    LMDebugPrint("\tuser:$instanceUser", debug)
    LMDebugPrint("\tinstanceURL:$instanceURL", debug)
    def returnArray = [:]
    def errors = []
    def connComplete
    def db_connection

    try
    {
        // Connection creation thrown into Try/Catch block as to quickly capture any issues with initial connection.
        // Create a connection to the database.
        String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        db_connection = Sql.newInstance(instanceURL, instanceUser, instancePass, driver)
        connComplete = true

    }
    catch (Exception e)
    {
        // Print out the exception and exit with 1.
        errors[0] = e.message
        LMDebugPrint("***** Attempt Connection error: ${e.message}", debug)
        connComplete = false
    }

    // populate the connection and any messages for the return array
    if (connComplete == true)
    {
        returnArray['connection'] = db_connection
        returnArray['status'] = 'success'
    }
    else
    {
        returnArray['status'] = 'failed'
        returnArray['errors'] = errors
        returnArray['connection'] = null
    }

    return returnArray
}


/*
Attempt to execute SQL command.
Returns an array.
the first element is the query data
second element is success or fail
third element is the error message
*/
def runQuery(String sqlQuery, conn, debug = false)
{
    def returnArray = [:]
    LMDebugPrint("****Running Query****", debug)
    LMDebugPrint("\tQuery to run: $sqlQuery", debug)
    // query the Oracle DB.
    try
    {
        returnArray['data'] = conn.rows(sqlQuery)
        returnArray['status'] = 'success'
        returnArray['error'] = ''
    }
    catch (Exception e)
    {
        returnArray['error'] = e.message
        returnArray['status'] = 'failed'
        LMDebugPrint("\tSQL Query Error message: ${e.message}", debug)
    }
    LMDebugPrint("Data Returned: ${returnArray.data}", debug)
    return returnArray
}