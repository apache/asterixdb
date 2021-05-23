<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->
 
 # AsterixDB Administration Console #
 
 ## <a id="toc">Table of Contents</a>
 
 * [Basics](#basics)
 * [Query Navigation](#qnav)
 * [Metadata Inspector](#metadatainspector)
 * [Interactive Plan Viewer](#planviewer)
 * [Exporting Data](#exporting)
 * [Development](#development)
 
## <a id="basics">Basic Usage</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

Executing a query on this console is easy. First, select from the input options and then select your execution mode.

__Input Options__

* `Dataverse` -  the dataverse that the query will use. The default is the `Default` dataverse. This is not required to
run a query and the console will try and autodetect the dataverse used in the query.
* `Plan Format` - specifies what format of the resulting query plan. 
    * `JSON` - results in the showing the interactive query plan viewer. 
    * `STRING` - results in the text/string format of the query plan. Equivalent to the text format from the old 19001 
    console.
* `Output Format` - the format of the result of the query.
    * `JSON` - the default and will return the results in JSON. Can also view in `Tree` and `Table` views in the output
    section.
    * `CSV (no header)` - will return CSV format but without the header. Can only view this in `Table` view in the output
    section.
    * `CSV (header)` - will return CSV format with the header. Can only view this in `Table` view in the output
    section. See the [Exporting Data](#exporting) section for more information and examples.

To execute the query click the green triangle in the bottom right. Users may also choose to click the `Explain` button.
This will not actually run the query (no results returned) and will only return the query plan. The console will default
the view in the output section to `Plan`.

To cancel the query click the red stop button in the bottom right. This will send a `DELETE` request to the server and cancel the previous
request.

The dashboard now supports autocomplete for SQL++ keywords. Use `CTRL+Space` to activate the autocomplete.

## <a id="qnav">Query Navigation</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

This console supports query history and has two different ways of navigating the query history. On the input bar there is a section
for `QUERY HISTORY` and there are also two arrows `<` and `>`.

Utilizing the arrows will let you traverse the queries one by one. However, if the console is already at the most recent query
in the history and the user clicks the `>` or forward arrow, it will create a new empty query. 

The `QUERY HISTORY` dropdown allows users to jump through the history without having to step through it with the arrows.

When executing a query, this query will be counted as a new query if it is different (purely the text of the query, not 
the results) from the most recent query. It will subsequently be added to the query history. 

## <a id="metadatainspector">Metadata Inspector</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

The metadata inspector is the column on the rightside of the console. The `Refresh` button is used to update the current metadata.
When a user creates or drops a Dataverse, Dataset, Datatype, or Index the changes will not be automatically reflected. User must
click the `Refresh` button to get the most up to date data. 

The console supports multiple dialogs/windows open at once. All of these are resizable and draggable as well.

Users can also click the `JSON` / `SUMMARY` button to toggle from the raw and parsed views. `SUMMARY` is the default.

#### Dataverse

Clicking a dataverse will add it to the shown metadata in this inspector. Users can select as many dataverses as desired.
The corresponding datasets, datatypes, and indices will appear.

#### Datasets

Clicking on a dataset will open a draggable and expandable window that contains information about the dataset.

* `Dataverse` - which dataverse the dataset belongs to.
* `Dataset` - the name of the dataset.
* `Datatype Name` - the name of the datatype of the dataset.
* `Primary Keys` - the primary keys of the dataset.
* `Sample` - if there is data inserted into the dataset, this is a section where viewers can see a sample of the dataset.
It is a `SELECT * FROM {dataset} LIMIT 1` query.

#### Datatypes

Clicking on a datatypes will open a draggable and expandable window that contains information about the datatype. This console
does support nested datatypes.

* `Dataverse` - which dataverse the datatype belongs to.
* `Datatype Name` - the name of the datatype.
* `Fields` - a list of the fields in the dataset. Each field has information on whether it is nullable or required. If the
field is nested / not a primitive type, click on it to see the information of that type. If the field is wrapped in `[ ]` or `{{ }}`,
then it is an ordered list or unordered of that type respectively. If a field is italicized, it is an anonymous type.

NOTE: the `JSON` view does not support nested like `SUMMARY` does.

#### Index

Clicking on a dataset will open a draggable and expandable window that contains information about the index.

* `Dataverse` - which dataverse the index belongs to.
* `Index Name` - the name of the index.
* `Index Type` - the type of the index.
* `Search Key(s)` - the key(s) of the index.



## <a id="planviewer">Interactive Plan Viewer</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

To view the interactive plan viewer, execute a query and switch to the `PLAN` tab in the output section. Alternatively,
users can click explain the query by clicking `EXPLAIN` instead of execute and the output section will default to the 
`PLAN` tab.

To interact with the query plan, drag to move the view of the graph. Use the scroll wheel or scroll movement to zoom in and out
of the plan. 

The default plan orientation is `Bottom to Top` and can be swapped for `Top to Bottom` if so desired.

The default view of the plan is not detailed (just operator IDs and operator names). To look at a more detailed plan, check
the `Detailed` checkbox and the plan will reload with more detail per node.

#### Traversing

There are multiple ways to traverse the query plan. the `Go to Node` dropdown will keep track of the currently selected
node. Using the arrows next to the `Go to Node` dropdown will traverse the plan node by node in a Depth First Search (DFS) 
fashion. Selecting nodes on the `Go to Node` dropdown will jump the plan to the selected node. 

Utilizing both the arrows and the `Go to Node` dropdown, it is easy to trace through a plan.

#### Search (Detailed mode only)

The `Search` function appears when the plan is in `Detailed` mode. Search for specific string occurrences in the plan. When
the search icon is clicked, the first mathc will be selected (if there is a match). Use the arrows that appear next to it
to iterate through every match. 

Must click `Clear Selections` after done with the search. 

Unfortunately, at this time regular expression search is not supported.

#### Variables (Detailed mode only)

The `See Variable Occurences` dropdown will appear when the plan is in `Detailed` mode. Users can select from any variable
that appears in the plan. Selecting a variable will jump to the node of last occurrence. The user can see how many occurence there
are by the `See Variable Occurences` dropdown title (it will now include a fraction). 

The arrows that appear can iterate through the occurences. 

Often, it is useful to be able to skip right to the declaration of a variable. By clicking on the skip button, the plan
will select the node where that variable was declared. To jump back to whatever node before, click the undo button.

#### Clear Selections

Clicking `Clear Selections` will reset the graph and focus on the first node in the plan.

## <a id="exporting">Exporting Data</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

### JSON/JSONL:

1. Select `JSON` in the input `Output Format` option and run the query that you want to export.
2. Click `Export` in the output section.
3. Select between `JSON` and `JSONL` (JSON Lines). Adjust the filename to the desired name.
4. Click `Export` to start the download. 

### CSV (no header):

1. Select `CSV (no header)` in the input `Output Format` option and run the query that you want to export.
2. Click `Export` in the output section.
3. Adjust the filename to the desired name.
4. Click `Export` to start the download.

### CSV (header):

1. Create a type that supports the query you want to run.
2. Set the `output-record-type` before your query
3. Select `CSV (no header)` in the input `Output Format` option and run the query that you want to export.
4. Click `Export` in the output section.
5. Adjust the filename to the desired name.
6. Click `Export` to start the download.

This one is trickier. In order to get the header in the CSV format, we need to set the `output-record-type` in the query
in order to get the headers. To explain further, here is an example using the TinySocial dataset from the Using SQL++ Primer.

    CREATE TYPE GleambookMessageType AS {
        messageId: int,
        authorId: int,
        inResponseTo: int?,
        senderLocation: point?,
        message: string
    };

    CREATE DATASET GleambookMessages(GleambookMessageType)
        PRIMARY KEY messageId;

If we wanted to export `messageId`, `authorId`, and `senderLocation` in CSV format with headers, we would have to create
an additional type to support this export.

    CREATE TYPE GleambookMessages_exportCSV AS {
        messageId: int,
        authorId: int,
        senderLocation: point
    };
    
The query would then look something like this:
    
    USE TinySocial;
    
    SET `output-record-type` "GleambookMessages_exportCSV";
    
    SELECT messageId, authorId, senderLocation
    FROM GleambookMessages;
    
Now run the query with the `CSV (header)` input option and the result will contain the hedaer `messageId`, `authorId`,
and `senderLocation`.

## <a id="development">Development</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

To start the development server, run `ng serve` or `npm start`. Navigate to `http://localhost:4200/`. 
The app will automatically reload if you change any of the source files.

To add a debugger, add a new `Javascrip Debug` configuration in the IntelliJ `Run Configurations` and set the URL to
`http://localhost:4200/`. Additionally, you can set the file directory to asterix-dashboard.