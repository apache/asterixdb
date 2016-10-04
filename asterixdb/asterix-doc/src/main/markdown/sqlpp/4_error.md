# <a id="Errors">4. Errors</a>
A SQL++ query can potentially result in one of the following errors:

 * syntax error,
 * identifier resolution error,
 * type error,
 * resource error.

If the query processor runs into any error, it will
terminate the ongoing processing of the query and
immediately return an error message to the client.

## <a id="Syntax_errors">Syntax Errors</a>
An valid SQL++ query must satisfy the SQL++ grammar rules.
Otherwise, a syntax error will be raised.

##### Example

    SELECT *
    FROM GleambookUsers user

Since the ending semi-colon is mandatory for any SQL++ query,
we will get a syntax error as follows:

    Error: Syntax error: In line 2 >>FROM GleambookUsers user<< Encountered <EOF> at column 24.
    ==> FROM GleambookUsers user

##### Example

    SELECT *
    FROM GleambookUsers user
    WHERE type="advertiser";

Since "type" a [reserved keyword](#Reserved_keywords) in the SQL++ parser,
we will get a syntax error as follows:

    Error: Syntax error: In line 3 >>WHERE type="advertiser";<< Encountered 'type' "type" at column 7.
    ==> WHERE type="advertiser";


## <a id="Identifier_resolution_errors">Identifier Resolution Errors</a>
Referring an undefined identifier can cause an error if the identifier
cannot be successfully resolved as a valid field access.

##### Example

    SELECT *
    FROM GleambookUser user;

Assume we have a typo in "GleambookUser" which misses the ending "s",
we will get an identifier resolution error as follows:

    Error: Cannot find dataset GleambookUser in dataverse Default nor an alias with name GleambookUser!

##### Example

    SELECT name, message
    FROM GleambookUsers u JOIN GleambookMessages m ON m.authorId = u.id;

If the compiler cannot figure out all possible fields in
`GleambookUsers` and `GleambookMessages`,
we will get an identifier resolution error as follows:

    Error: Cannot resolve ambiguous alias reference for undefined identifier name


## <a id="Type_errors">Type Errors</a>

The SQL++ compiler does type checks based on its available type information.
In addition, the SQL++ runtime also reports type errors if a data model instance
it processes does not satisfy the type requirement.

##### Example

    abs("123");

Since function `abs` can only process numeric input values,
we will get a type error as follows:

    Error: Arithmetic operations are not implemented for string


## <a id="Resource_errors">Resource Errors</a>
A query can potentially exhaust system resources, such
as the number of open files and disk spaces.
For instance, the following two resource errors could be potentially
be seen when running the system:

    Error: no space left on device
    Error: too many open files

The "no space left on device" issue usually can be fixed by
cleaning up disk spaces and reserving more disk spaces for the system.
The "too many open files" issue usually can be fixed by a system
administrator, following the instructions
[here](https://easyengine.io/tutorials/linux/increase-open-files-limit/).

