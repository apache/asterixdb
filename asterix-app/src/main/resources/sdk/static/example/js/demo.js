$(document).ready(function() {

    // 8 - Simple Aggregation
    $('#run8').click(function () {

        // Option 1: Simple, Object Syntax     
        $('#result8').html('');   
        var expression8 = new FunctionExpression({
            "function"      : "count",
            "expression"    : new ForClause("fbu", null, new AsterixExpression().set(["dataset FacebookUsers"])).bind({"return" : "$fbu"}),
            "dataverse"     : "TinySocial",
            "success"       : function(res) {
                                $('#result8').html(res["results"]);
                              }
        });
        expression8.run();
    });
    
});
