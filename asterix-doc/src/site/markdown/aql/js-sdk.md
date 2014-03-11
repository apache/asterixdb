# AsterixDB Javascript SDK #

## Installing/Including ##
... TODO ...

## Learning the javascript SDK--by example ##
In this section, we explore how to form AQL queries using the javascript SDK.
... TODO ...

### Query 0-A - Exact-Match Lookup ###

        use dataverse TinySocial;

        for $user in dataset FacebookUsers
        where $user.id = 8
        return $user;

... TODO ...

        var expression0a = new FLWOGRExpression()
            .ForClause("$user", new AExpression("dataset FacebookUsers"))
            .WhereClause(new AExpression("$user.id = 8"))
            .ReturnClause("$user");

