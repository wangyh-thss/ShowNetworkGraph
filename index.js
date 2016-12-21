$(function() {
    var myChart = echarts.init(document.getElementById('main'));
    myChart.showLoading();

    var egoNodeSize = {};
    var edges = [];
    var nodes = [];
    var egoNets = {};
    var edgeIndex = {};
    var boldEdges = [];

    var colorPalette = ['#E01F54','#001852','#f5e8c8','#b8d2c7','#c6b38e',
        '#a4d8c2','#f3d999','#d3758f','#dcc392','#2e4783',
        '#82b6e9','#ff6347','#a092f1','#0a915d','#eaf889',
        '#6699FF','#ff6666','#3cb371','#d5b158','#38b6b6'
    ];

    var getRandom = function(max) {
        return Math.random() * max;
    }

    var getRandomColor = function() {
        return colorPalette[Math.floor(getRandom(colorPalette.length))]
    }

    var computeNodeSize = function(nid) {
        if (!egoNodeSize[nid]) {
            return 5;
        }
        return 10 + Math.log(egoNodeSize[nid]);
    }

    var edgeHash = function(source, target) {
        return source + '_' + target;
    }

    var getOption = function(nodes, edges) {
        return {
            series: [{
                type: 'graph',
                layout: 'none',
                animation: false,
                draggable: false,
                data: nodes,
                categories: [{
                    name: 'Normal'
                }, {
                    name: 'EgoNode'
                }],
                force: {
                    edgeLength: 10,
                    repulsion: 50,
                    gravity: 0.1
                },
                edges: edges
            }]
        }
    }

    var updateGraph = function(nodes, edges) {
        var option = getOption(nodes, edges);
        myChart.setOption(option);
    }

    $.get('data/facebook_result.txt', function(data) {
        myChart.hideLoading();
        var lines = data.split('\n');
        var nodesDict = {};
        var tempEgoNode = undefined;
        var lenList = [];
        var reg = new RegExp(/\((\d*), (\d*)/);
        var edgesCount = 0;
        for (var line of lines) {
            if (line == '') {
                continue;
            }
            if (line.startsWith('EgoNode')) {
                tempEgoNode = line.split(': ')[1];
            }
            if (line.startsWith('(')) {
                var nodePairs = line.split('),');
                // lenList.push(nodePairs.length);
                if (nodePairs.length != 67) {
                    continue;
                }
                var egoEdges = [];
                for (var nodePair of nodePairs) {
                    var matchResult = nodePair.match(reg);
                    if (!matchResult || matchResult.length != 3) {
                        continue;
                    }
                    var source = matchResult[1];
                    var target = matchResult[2];
                    nodesDict[source] = nodesDict[source] || 1;
                    nodesDict[target] = nodesDict[target] || 1;
                    var type = 0;
                    if (source == tempEgoNode || target == tempEgoNode) {
                        type = 1;
                    }
                    edges.push({
                        source: source,
                        target: target,
                        type: type
                    });
                    var key = edgeHash(source, target);
                    edgeIndex[key] = edgesCount;
                    egoEdges.push(key);
                    edgesCount++;
                }
                nodesDict[tempEgoNode] = 2;
                egoNodeSize[tempEgoNode] = nodePairs.length;
                egoNets[tempEgoNode] = egoEdges;
            }
        }
        nodes = Object.keys(nodesDict).map(function(nid) {
            var category = 0;
            if (nodesDict[nid] == 2) {
                category = 1
            }
            return {
                id: nid,
                category: category,
                name: nid,
                symbolSize: computeNodeSize(nid),
                x: getRandom(myChart.getWidth()),
                y: getRandom(myChart.getHeight()),
                itemStyle: {
                    normal: {
                        color: getRandomColor()
                    }
                }
            };
        });

        updateGraph(nodes, edges);

        myChart.on('click', function(evt) {
            if (evt.data.category != 1) {
                return;
            }
            var nid = evt.data.name;
            for (var boldEdgeIndex of boldEdges) {
                delete edges[boldEdgeIndex].lineStyle;
            }
            boldEdges = [];
            for (var edgeKey of egoNets[nid]) {
                var index = edgeIndex[edgeKey];
                var edgeObj = edges[index];
                var color = 'blue';
                var width = 2;
                if (edgeObj.type == 0) {
                    color = 'red';
                    width = 5;
                }
                edges[index] = $.extend(edgeObj, {
                    lineStyle: {
                        normal: {
                            width: width,
                            color: color
                        }
                    }
                });
                boldEdges.push(index);
            }
            updateGraph(nodes, edges);
        })
    })
});