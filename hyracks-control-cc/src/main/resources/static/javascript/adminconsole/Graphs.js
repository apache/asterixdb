Graphs = {};

Graphs.DAG = function() {
    this.nodeMap = {};
    this.edgeMap = {};
}

Graphs.DAG.prototype.addNode = function(key, attachment) {
    var node = new Graphs.Node(this, key, attachment);
    this.nodeMap[key] = node;
    return node;
}

Graphs.DAG.prototype.addEdge = function(key, attachment, sNode, sIndex, tNode, tIndex) {
    var edge = new Graphs.DirectedEdge(this, key, attachment, sNode, sIndex, tNode, tIndex);
    this.edgeMap[key] = edge;
    sNode.outEdges[sIndex] = edge;
    tNode.inEdges[tIndex] = edge;
    return edge;
}

Graphs.DAG.prototype.lookupNode = function(key) {
    return this.nodeMap[key];
}

Graphs.DAG.prototype.lookupEdge = function(key) {
    return this.edgeMap[key];
}

Graphs.DAG.prototype.walkNodes = function(callback) {
    for ( var nKey in this.nodeMap) {
        callback(this.nodeMap[nKey]);
    }
}

Graphs.DAG.prototype.walkEdges = function(callback) {
    for ( var eKey in this.edgeMap) {
        callback(this.edgeMap[eKey]);
    }
}

Graphs.DAG.prototype.findRoots = function() {
    var roots = [];
    var callback = function(node) {
        if (node.inEdges.length == 0) {
            roots.push(node);
        }
    }
    this.walkNodes(callback);
    return roots;
}

Graphs.DAG.prototype.findLeaves = function() {
    var leaves = [];
    var callback = function(node) {
        if (node.outEdges.length == 0) {
            leaves.push(node);
        }
    }
    this.walkNodes(callback);
    return leaves;
}

Graphs.DAG.prototype.tsort = function() {
    var sortedNodes = [];
    var nodeState = {};

    function visit(node) {
        if (!nodeState[node.key]) {
            nodeState[node.key] = true;
            for ( var i = 0; i < node.inEdges.length; ++i) {
                visit(node.inEdges[i].sNode);
            }
            sortedNodes.push(node);
        }
    }

    var roots = this.findLeaves();
    for ( var i = 0; i < roots.length; ++i) {
        visit(roots[i]);
    }
    return sortedNodes;
}

Graphs.DAG.prototype.stratify = function() {
    var sortedNodes = this.tsort();
    var stratumMap = {};
    var strata = [];
    for ( var i = 0; i < sortedNodes.length; ++i) {
        var node = sortedNodes[i];
        var maxParentStratum = -1;
        for ( var j = 0; j < node.inEdges.length; ++j) {
            var edge = node.inEdges[j];
            maxParentStratum = Math.max(maxParentStratum, stratumMap[edge.sNode.key]);
        }
        var stratum = maxParentStratum + 1;
        stratumMap[node.key] = stratum;
        var stratumList = strata[stratum];
        if (!stratumList) {
            stratumList = [];
            strata[stratum] = stratumList;
        }
        stratumList.push(node);
    }
    return strata;
}

Graphs.Node = function(dag, key, attachment) {
    this.dag = dag;
    this.key = key;
    this.attachment = attachment;
    this.inEdges = [];
    this.outEdges = [];
}

Graphs.DirectedEdge = function(dag, key, attachment, sNode, sIndex, tNode, tIndex) {
    this.dag = dag;
    this.key = key;
    this.attachment = attachment;
    this.sNode = sNode;
    this.sIndex = sIndex;
    this.tNode = tNode;
    this.tIndex = tIndex;
}

Graphs.JsPlumbRenderer = function(dag, element, options) {
    this.dag = dag;
    this.element = element;
    this.options = options || {
        levelStep : 100
    };
}

Graphs.JsPlumbRenderer.prototype.configureDiv = function(div, node, stratum, inStratumIndex, stratumWidth) {
    div.id = node.key;
    div.dagNode = node;
    /*
     * div.style.position = 'absolute'; div.style.left = (stratum *
     * this.options.levelStep) + 'px'; div.style.top = (inStratumIndex * 100) +
     * 'px'; div.style.width = '50px'; div.style.height = '50px';
     * div.style.border = 'thick solid #000000';
     */
}

Graphs.JsPlumbRenderer.prototype.refresh = function() {
    var strata = this.dag.stratify();

    while (this.element.hasChildNodes()) {
        this.element.removeChild(this.element.lastChild);
    }
    for ( var i = 0; i < strata.length; ++i) {
        var stratumList = strata[i];
        for ( var j = 0; j < stratumList.length; ++j) {
            var node = stratumList[j];
            var div = document.createElement('div');
            this.configureDiv(div, node, i, j, stratumList.length);
            this.element.appendChild(div);
        }
    }
}