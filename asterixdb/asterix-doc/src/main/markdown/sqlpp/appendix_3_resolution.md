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

In this Appendix, we'll look at how variables are bound and how names are resolved.
Names can appear in every clause of a query.
Sometimes a name consists of just a single identifier, e.g., `region` or `revenue`.
More often a name will consist of two identifiers separated by a dot, e.g., `customer.address`.
Occasionally a name may have more than two identifiers, e.g., `policy.owner.address.zipcode`.
*Resolving* a name means determining exactly what the (possibly multi-part) name refers to.
It is necessary to have well-defined rules for how to resolve a name in cases of ambiguity.
(In the absence of schemas, such cases arise more commonly, and also differently, than they do in SQL.)

The basic job of each clause in a query block is to bind variables.
Each clause sees the variables bound by previous clauses and may bind additional variables.
Names are always resolved with respect to the variables that are bound ("in scope") at the place where the name use in question occurs.
It is possible that the name resolution process will fail, which may lead to an empty result or an error message.

One important bit of background: When the system is reading a query and resolving its names, it has a list of all the available dataverses and datasets.
As a result, it knows whether `a.b` is a valid name for dataset `b` in dataverse `a`.
However, the system does not in general have knowledge of the schemas of the data inside the datasets; remember that this is a much more open world.
As a result, in general the system cannot know whether any object in a particular dataset will have a field named `c`.
These assumptions affect how errors are handled.
If you try to access dataset `a.b` and no dataset by that name exists, you will get an error and your query will not run.
However, if you try to access a field `c` in a collection of objects, your query will run and return `missing` for each object that doesn't have a field named `c` – this is because it’s possible that some object (someday) could have such a field.

## <a id="Binding_variables">Binding Variables</a>

Variables can be bound in the following ways:

1.  WITH and LET clauses bind a variable to the result of an expression in a straightforward way

    Examples:

    `WITH cheap_parts AS (SELECT partno FROM parts WHERE price < 100)`
    binds the variable `cheap_parts` to the result of the subquery.

    `LET pay = salary + bonus`
    binds the variable `pay` to the result of evaluating the expression `salary + bonus`.

2.  FROM, GROUP BY, and SELECT clauses have optional AS subclauses that contain an expression and a name (called an *iteration variable* in a FROM clause, or an alias in GROUP BY or SELECT.)

    Examples:

    `FROM customer AS c, order AS o`

    `GROUP BY salary + bonus AS total_pay`

    `SELECT MAX(price) AS highest_price`

    An AS subclause always binds the name (as a variable) to the result of the expression (or, in the case of a FROM clause, to the *individual members* of the collection identified by the expression.)

    It's always a good practice to use the keyword AS when defining an alias or iteration variable.
    However, as in SQL, the syntax allows the keyword AS to be omitted.
    For example, the FROM clause above could have been written like this:

    `FROM customer c, order o`

    Omitting the keyword AS does not affect the binding of variables.
    The FROM clause in this example binds variables c and o whether the keyword AS is used or not.

    In certain cases, a variable is automatically bound even if no alias or variable-name is specified.
    Whenever an expression could have been followed by an AS subclause, if the expression consists of a simple name or a path expression, that expression binds a variable whose name is the same as the simple name or the last step in the path expression.
    Here are some examples:

    `FROM customer, order` binds iteration variables named `customer` and `order`

    `GROUP BY address.zipcode` binds a variable named `zipcode`

    `SELECT item[0].price` binds a variable named `price`

    Note that a FROM clause iterates over a collection (usually a dataset), binding a variable to each member of the collection in turn.
    The name of the collection remains in scope, but it is not a variable.
    For example, consider this FROM clause used in a self-join:

    `FROM customer AS c1, customer AS c2`

    This FROM clause joins the customer dataset to itself, binding the iteration variables c1 and c2 to objects in the left-hand-side and right-hand-side of the join, respectively.
    After the FROM clause, c1 and c2 are in scope as variables, and customer remains accessible as a dataset name but not as a variable.

3.  Special rules for GROUP BY:

    1.  If a GROUP BY clause specifies an expression that has no explicit alias, it binds a pseudo-variable that is lexicographically identical to the expression itself.
        For example:

        `GROUP BY salary + bonus` binds a pseudo-variable named `salary + bonus`.

        This rule allows subsequent clauses to refer to the grouping expression (salary + bonus) even though its constituent variables (salary and bonus) are no longer in scope.
        For example, the following query is valid:

            FROM employee
            GROUP BY salary + bonus
            HAVING salary + bonus > 1000
            SELECT salary + bonus, COUNT(*) AS how_many

        While it might have been more elegant to explicitly require an alias in cases like this, the pseudo-variable rule is retained for SQL compatibility.
        Note that the expression `salary + bonus` is not *actually* evaluated in the HAVING and SELECT clauses (and could not be since `salary` and `bonus` are no longer individually in scope).
        Instead, the expression `salary + bonus` is treated as a reference to the pseudo-variable defined in the GROUP BY clause.

    2.  A GROUP BY clause may be followed by a GROUP AS clause that binds a variable to the group.
        The purpose of this variable is to make the individual objects inside the group visible to subqueries that may need to iterate over them.

        The GROUP AS variable is bound to a multiset of objects.
        Each object represents one of the members of the group.
        Since the group may have been formed from a join, each of the member-objects contains a nested object for each variable bound by the nearest FROM clause (and its LET subclause, if any).
        These nested objects, in turn, contain the actual fields of the group-member.
        To understand this process, consider the following query fragment:

            FROM parts AS p, suppliers AS s
            WHERE p.suppno = s.suppno
            GROUP BY p.color GROUP AS g

        Suppose that the objects in `parts` have fields `partno`, `color`, and `suppno`.
        Suppose that the objects in suppliers have fields `suppno` and `location`.

        Then, for each group formed by the GROUP BY, the variable g will be bound to a multiset with the following structure:

            [ { "p": { "partno": "p1", "color": "red", "suppno": "s1" },
                "s": { "suppno": "s1", "location": "Denver" } },
              { "p": { "partno": "p2", "color": "red", "suppno": "s2" },
                "s": { "suppno": "s2", "location": "Atlanta" } },
              ...
            ]

## <a id="Scoping">Scoping</a>

In general, the variables that are in scope at a particular position are those variables that were bound earlier in the current query block, in outer (enclosing) query blocks, or in a WITH clause at the beginning of the query.
More specific rules follow.

The clauses in a query block are conceptually processed in the following order:

* FROM (followed by LET subclause, if any)
* WHERE
* GROUP BY (followed by LET subclause, if any)
* HAVING
* SELECT or SELECT VALUE
* ORDER BY
* OFFSET
* LIMIT

During processing of each clause, the variables that are in scope are those variables that are bound in the following places:

1.  In earlier clauses of the same query block (as defined by the ordering given above).

    Example: `FROM orders AS o SELECT o.date`
    The variable `o` in the SELECT clause is bound, in turn, to each object in the dataset `orders`.

2.  In outer query blocks in which the current query block is nested.
    In case of duplication, the innermost binding wins.

3.  In the WITH clause (if any) at the beginning of the query.

However, in a query block where a GROUP BY clause is present:

1.  In clauses processed before GROUP BY, scoping rules are the same as though no GROUP BY were present.

2.  In clauses processed after GROUP BY, the variables bound in the nearest FROM-clause (and its LET subclause, if any) are removed from scope and replaced by the variables bound in the GROUP BY clause (and its LET subclause, if any).
    However, this replacement does not apply inside the arguments of the five SQL special aggregating functions (MIN, MAX, AVG, SUM, and COUNT).
    These functions still need to see the individual data items over which they are computing an aggregation.
    For example, after `FROM employee AS e GROUP BY deptno`, it would not be valid to reference `e.salary`, but `AVG(e.salary)` would be valid.

Special case: In an expression inside a FROM clause, a variable is in scope if it was bound in an earlier expression in the same FROM clause.
Example:

    FROM orders AS o, o.items AS i

The reason for this special case is to support iteration over nested collections.

Note that, since the SELECT clause comes *after* the WHERE and GROUP BY clauses in conceptual processing order, any variables defined in SELECT are not visible in WHERE or GROUP BY.
Therefore the following query will not return what might be the expected result (since in the WHERE clause, `pay` will be interpreted as a field in the `emp` object rather than as the computed value `salary + bonus`):

    SELECT name, salary + bonus AS pay
    FROM emp
    WHERE pay > 1000
    ORDER BY pay

The likely intent of the query above can be accomplished as follows:

    FROM emp AS e
    LET pay = e.salary + e.bonus
    WHERE pay > 1000
    SELECT e.name, pay
    ORDER BY pay

Note that variables defined by `JOIN` subclauses are not visible to other subclauses in the same `FROM` clause.
This also applies to the `FROM` variable that starts the `JOIN` subclause.

## <a id="Resolving_names">Resolving Names</a>

The process of name resolution begins with the leftmost identifier in the name.
The rules for resolving the leftmost identifier are:

1.  _In a FROM clause_: Names in a FROM clause identify the collections over which the query block will iterate.
    These collections may be stored datasets or may be the results of nested query blocks.
    A stored dataset may be in a named dataverse or in the default dataverse.
    Thus, if the two-part name `a.b` is in a FROM clause, a might represent a dataverse and `b` might represent a dataset in that dataverse.
    Another example of a two-part name in a FROM clause is `FROM orders AS o, o.items AS i`.
    In `o.items`, `o` represents an order object bound earlier in the FROM clause, and items represents the items object inside that order.

    The rules for resolving the leftmost identifier in a FROM clause (including a JOIN subclause), or in the expression following IN in a quantified predicate, are as follows:

    1.  If the identifier matches a variable-name that is in scope, it resolves to the binding of that variable.
        (Note that in the case of a subquery, an in-scope variable might have been bound in an outer query block; this is called a correlated subquery.)

    2.  Otherwise, if the identifier is the first part of a two-part name like `a.b`, the name is treated as dataverse.dataset.
        If the identifier stands alone as a one-part name, it is treated as the name of a dataset in the default dataverse.
        An error will result if the designated dataverse or dataset does not exist.

2.  _Elsewhere in a query block_: In clauses other than FROM, a name typically identifies a field of some object.
    For example, if the expression `a.b` is in a SELECT or WHERE clause, it's likely that `a` represents an object and `b` represents a field in that object.

    The rules for resolving the leftmost identifier in clauses other than the ones listed in Rule 1 are:

    1.  If the identifier matches a variable-name that is in scope, it resolves to the binding of that variable.
        (In the case of a correlated subquery, the in-scope variable might have been bound in an outer query block.)

    2.  (The "Single Variable Rule"): Otherwise, if the FROM clause in the current query block binds exactly one variable, the identifier is treated as a field access on the object bound to that variable.
        For example, in the query `FROM customer SELECT address`, the identifier address is treated as a field in the object bound to the variable customer.
        At runtime, if the object bound to customer has no `address` field, the `address` expression will return `missing`.
        If the FROM clause in the current query block binds multiple variables, name resolution fails with an "ambiguous name" error.
        If there's no FROM clause in the current query block, name resolution fails with an "undefined identifier" error.
        Note that the Single Variable Rule searches for bound variables only in the current query block, not in outer (containing) blocks.
        The purpose of this rule is to permit the compiler to resolve field-references unambiguously without relying on any schema information.
        Also note that variables defined by LET clauses do not participate in the resolution process performed by this rule.

        Exception: In a query that has a GROUP BY clause, the Single Variable Rule does not apply in any clauses that occur after the GROUP BY because, in these clauses, the variables bound by the FROM clause are no longer in scope.
        In clauses after GROUP BY, only Rule 2.1 applies.

3.  In an ORDER BY clause following a UNION ALL expression:

    The leftmost identifier is treated as a field-access on the objects that are generated by the UNION ALL.
    For example:

        query-block-1
        UNION ALL
        query-block-2
        ORDER BY salary

    In the result of this query, objects that have a foo field will be ordered by the value of this field; objects that have no foo field will appear at at the beginning of the query result (in ascending order) or at the end (in descending order.)

4.  _In a standalone expression_: If a query consists of a standalone expression then identifiers inside that
    expression are resolved according to Rule 1.
    For example, if the whole query is `ARRAY_COUNT(a.b)` then `a.b` will be treated as dataset `b` contained in
    dataverse `a`.
    Note that this rule only applies to identifiers which are located directly inside a standalone expression.
    Identifiers inside SELECT statements in a standalone expression are still resolved according to Rules 1-3.
    For example, if the whole query is `ARRAY_SUM( (FROM employee AS e SELECT VALUE salary) )` then `salary` is resolved
    as `e.salary` following the "Single Variable Rule" (Rule 2.2).

5.  Once the leftmost identifier has been resolved, the following dots and identifiers in the name (if any) are treated as a path expression that navigates to a field nested inside that object.
    The name resolves to the field at the end of the path.
    If this field does not exist, the value `missing` is returned.
