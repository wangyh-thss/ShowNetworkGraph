$(function() {
    var myChart = echarts.init(document.getElementById('main'));
    myChart.showLoading();
    $.get('data/0.edges', function(data) {
        myChart.hideLoading();
        var edgesStr = data.split('\n');
        var nodesDict = {};
        var edges = [];
        for (var edge of edgesStr) {
            if (edge == '') {
                continue;
            }
            var nodesPair = edge.split(' ');
            nodesDict[nodesPair[0]] = 1;
            nodesDict[nodesPair[1]] = 1;
            edges.push({
                source: nodesPair[0],
                target: nodesPair[1]
            });
        }
        var nodes = Object.keys(nodesDict).map(function(nid) {
            return {id: nid};
        });
        var option = {
            series: [{
                type: 'graph',
                layout: 'force',
                animation: false,
                draggable: false,
                data: nodes,
                force: {
                    edgeLength: 5,
                    repulsion: 20,
                    gravity: 0.2
                },
                edges: edges
            }]
        }
        myChart.setOption(option);
    })
});