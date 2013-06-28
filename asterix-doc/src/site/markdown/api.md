# REST API to AsterixDB #

## DDL API ##

*End point for the data definition statements*

Endpoint: _/ddl_

Parameters:

<table>
<tr>
  <td>Parameter</td>
  <td>Description</td>
  <td>Required?</td>
</tr>
<tr>
  <td>ddl</td>
  <td>String containing DDL statements to modify Metadata</td>
  <td>Yes</td>
</tr>
</table>

This call does not return any result. If the operations were successful, HTTP OK status code is returned.

### Example ###

#### DDL Statements ####


        drop dataverse company if exists;
        create dataverse company;
        use dataverse company;
        
        create type Emp as open {
          id : int32,
          name : string
        };
        
        create dataset Employee(Emp) primary key id;


API call for the above DDL statements in the URL-encoded form.

[http://localhost:19002/ddl?ddl=drop%20dataverse%20company%20if%20exists;create%20dataverse%20company;use%20dataverse%20company;create%20type%20Emp%20as%20open%20{id%20:%20int32,name%20:%20string};create%20dataset%20Employee(Emp)%20primary%20key%20id;](http://localhost:19002/ddl?ddl=drop%20dataverse%20company%20if%20exists;create%20dataverse%20company;use%20dataverse%20company;create%20type%20Emp%20as%20open%20{id%20:%20int32,name%20:%20string};create%20dataset%20Employee(Emp)%20primary%20key%20id;)

#### Response ####
*HTTP OK 200*  
`<NO PAYLOAD>`

## Update API ##

*End point for update statements (INSERT, DELETE and LOAD)*

Endpoint: _/update_

Parameters:

<table>
<tr>
  <td>Parameter</td>
  <td>Description</td>
  <td>Required?</td>
</tr>
<tr>
  <td>statements</td>
  <td>String containing update (insert/delete) statements to execute</td>
  <td>Yes</td>
</tr>
</table>

This call does not return any result. If the operations were successful, HTTP OK status code is returned.

### Example ###

#### Update Statements ####


        use dataverse company;
        
        insert into dataset Employee({ "id":123,"name":"John Doe"});


API call for the above update statement in the URL-encoded form.

[http://localhost:19002/update?statements=use%20dataverse%20company;insert%20into%20dataset%20Employee({%20%22id%22:123,%22name%22:%22John%20Doe%22});](http://localhost:19002/update?statements=use%20dataverse%20company;insert%20into%20dataset%20Employee({%20%22id%22:123,%22name%22:%22John%20Doe%22});)

#### Response ####
*HTTP OK 200*  
`<NO PAYLOAD>`

## Query API ##

*End point for query statements*

Endpoint: _/query_

Parameters:

<table>
<tr>
  <td>Parameter</td>
  <td>Description</td>
  <td>Required?</td>
</tr>
<tr>
  <td>query</td>
  <td>Query string to pass to ASTERIX for execution</td>
  <td>Yes</td>
</tr>
<tr>
  <td>mode</td>
  <td>Indicate if call should be synchronous or asynchronous. mode = synchronous blocks the call until results are available; mode = asynchronous returns immediately with a handle that can be used later to check the query’s status and to fetch results when available</td>
  <td>No. default mode = synchronous</td>
</tr>
</table>

Result: The result is returned as a JSON object as follows


        {
           results: <result as a string, if mode = synchronous>
           error-code: [<code>, <message>] (if an error occurs)
           handle: <opaque result handle, if mode = asynchronous>
        }


### Example ###

#### Select query with synchronous result delivery ####


        use dataverse company;
        
        for $l in dataset('Employee') return $l;


API call for the above query statement in the URL-encoded form.

[http://localhost:19002/query?query=use%20dataverse%20company;for%20$l%20in%20dataset('Employee')%20return%20$l;](http://localhost:19002/query?query=use%20dataverse%20company;for%20$l%20in%20dataset('Employee')%20return%20$l;)

#### Response ####
*HTTP OK 200*  
Payload


        {
          "results": [
              [
                  "{ "id": 123, "name": "John Doe" }"
              ]
          ]
        }


#### Same select query with asynchronous result delivery ####

API call for the above query statement in the URL-encoded form with mode=asynchronous

[http://localhost:19002/query?query=use%20dataverse%20company;for%20$l%20in%20dataset('Employee')%20return%20$l;&amp;mode=asynchronous](http://localhost:19002/query?query=use%20dataverse%20company;for%20$l%20in%20dataset('Employee')%20return%20$l;&amp;mode=asynchronous)

#### Response ####
*HTTP OK 200*  
Payload


        {
            "handle": [45,0]
        }


## Asynchronous Result API ##

*End point to fetch the results of an asynchronous query*

Endpoint: _/query/result_

Parameters:

<table>
<tr>
  <td>Parameter</td>
  <td>Description</td>
  <td>Required?</td>
</tr>
<tr>
  <td>handle</td>
  <td>Result handle that was returned by a previous call to a /query call with mode = asynchronous</td>
  <td>Yes</td>
</tr>
</table>

Result: The result is returned as a JSON object as follows:


        {
           results: <result as a string, if mode = synchronous, or mode = asynchronous and results are available>
           error-code: [<code>, <message>] (if an error occurs)
        }


If mode = asynchronous and results are not available, the returned JSON object is empty: { }

### Example ###

#### Fetching results for asynchronous query ####

We use the handle returned by the asynchronous query to get the results for the query. The handle returned was:


        {
            "handle": [45,0]
        }


API call for reading results from the previous asynchronous query in the URL-encoded form.

[http://localhost:19002/query/result?handle=%7B%22handle%22%3A+%5B45%2C+0%5D%7D](http://localhost:19002/query/result?handle=%7B%22handle%22%3A+%5B45%2C+0%5D%7D)

#### Response ####
*HTTP OK 200*  
Payload


        {
          "results": [
              [
                  "{ "id": 123, "name": "John Doe" }"
              ]
          ]
        }


## Query Status API ##

*End point to check the status of the query asynchronous*

Endpoint: _/query/status_

Parameters:

<table>
<tr>
  <td>Parameter</td>
  <td>Description</td>
  <td>Required?</td>
</tr>
<tr>
  <td>handle</td>
  <td>Result handle that was returned by a previous call to a /query call with mode = asynchronous</td>
  <td>Yes</td>
</tr>
</table>

Result: The result is returned as a JSON object as follows:


        {
           status: ("RUNNING" | "SUCCESS" | "ERROR")
        }



## Error Codes ##

Table of error codes and their types:

<table>
<tr>
  <td>Code</td>
  <td>Type</td>
</tr>
<tr>
  <td>1</td>
  <td>Invalid statement</td>
</tr>
<tr>
  <td>2</td>
  <td>Parse failures</td>
</tr>
<tr>
  <td>99</td>
  <td>Uncategorized error</td>
</tr>
</table>
