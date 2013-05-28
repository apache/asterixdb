$(function() {

    // Exact-Match Lookup
    $('#run0a').click(function () {
        var a = new AsterixSDK();
        var q = new FLWOGRExpression()
            .use_dataverse("Twitter")
            .bind(new ForClause("user", new AsterixClause().set("dataset FacebookUsers")))
            .bind(new WhereClause(new BooleanExpression("=", "$user.id", 8)))
            .return({ "$user" : "$user" });
        alert(q.clauses);
    });

    // Range Scan
    $('#run0b').click(function () {
        alert("0b");
    });
    
    // Other Query Filter
    $('#run1').click(function () {
        alert("1");
    });

});

// Bootstraps social data
function bootstrap() {
    var loc = window.location.pathname;
    var dir = loc.substring(0, loc.lastIndexOf('/'));
}
