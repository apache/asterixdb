$(function() {
    var options = {
        lines : {
            show : true
        },
        points : {
            show : false
        },
        xaxis : {
            tickDecimals : 0,
            tickSize : 0
        }
    };

    function onDataReceived(data) {
        var result = data.result;
        var sysLoad = result['system-load-averages'];
        var heapUsageInitSizes = result['heap-init-sizes'];
        var heapUsageUsedSizes = result['heap-used-sizes'];
        var heapUsageCommittedSizes = result['heap-committed-sizes'];
        var heapUsageMaxSizes = result['heap-max-sizes'];
        var nonheapUsageInitSizes = result['nonheap-init-sizes'];
        var nonheapUsageUsedSizes = result['nonheap-used-sizes'];
        var nonheapUsageCommittedSizes = result['nonheap-committed-sizes'];
        var nonheapUsageMaxSizes = result['nonheap-max-sizes'];
        var threadCounts = result['thread-counts'];
        var peakThreadCounts = result['peak-thread-counts'];
        var gcCollectionCounts = result['gc-collection-counts'];
        var gcCollectionTimes = result['gc-collection-times'];
        var sysLoadArray = [];
        var heapUsageInitSizesArray = [];
        var heapUsageUsedSizesArray = [];
        var heapUsageCommittedSizesArray = [];
        var heapUsageMaxSizesArray = [];
        var nonheapUsageInitSizesArray = [];
        var nonheapUsageUsedSizesArray = [];
        var nonheapUsageCommittedSizesArray = [];
        var nonheapUsageMaxSizesArray = [];
        var threadCountsArray = [];
        var peakThreadCountsArray = [];
        var gcCollectionCountsArray = [];
        var gcCollectionTimesArray = [];
        var gcChartsDiv = document.getElementById('gc-charts');
        for ( var i = 0; i < gcCollectionCounts.length; ++i) {
            gcCollectionCountsArray.push([]);
            gcCollectionTimesArray.push([]);
            var gcCollectionCountDiv = document.createElement('div');
            gcCollectionCountDiv.id = 'gc-collection-count-' + i;
            gcCollectionCountDiv.style = "jqplot-target time-chart";
            gcChartsDiv.appendChild(gcCollectionCountDiv);
            var gcCollectionTimeDiv = document.createElement('div');
            gcCollectionTimeDiv.id = 'gc-collection-time-' + i;
            gcChartsDiv.appendChild(gcCollectionTimeDiv);
        }
        var rrdPtr = result['rrd-ptr'];
        for ( var i = 0; i < sysLoad.length; ++i) {
            sysLoadArray.push([ i, sysLoad[rrdPtr] ]);
            heapUsageInitSizesArray.push([ i, heapUsageInitSizes[rrdPtr] / (1024 * 1024) ]);
            heapUsageUsedSizesArray.push([ i, heapUsageUsedSizes[rrdPtr] / (1024 * 1024) ]);
            heapUsageCommittedSizesArray.push([ i, heapUsageCommittedSizes[rrdPtr] / (1024 * 1024) ]);
            heapUsageMaxSizesArray.push([ i, heapUsageMaxSizes[rrdPtr] / (1024 * 1024) ]);
            nonheapUsageInitSizesArray.push([ i, nonheapUsageInitSizes[rrdPtr] / (1024 * 1024) ]);
            nonheapUsageUsedSizesArray.push([ i, nonheapUsageUsedSizes[rrdPtr] / (1024 * 1024) ]);
            nonheapUsageCommittedSizesArray.push([ i, nonheapUsageCommittedSizes[rrdPtr] / (1024 * 1024) ]);
            nonheapUsageMaxSizesArray.push([ i, nonheapUsageMaxSizes[rrdPtr] / (1024 * 1024) ]);
            threadCountsArray.push([ i, threadCounts[rrdPtr] ]);
            peakThreadCountsArray.push([ i, peakThreadCounts[rrdPtr] ]);
            for ( var j = 0; j < gcCollectionCounts.length; ++j) {
                gcCollectionCountsArray[j].push([ i, gcCollectionCounts[j][rrdPtr] ]);
            }
            for ( var j = 0; j < gcCollectionTimes.length; ++j) {
                gcCollectionTimesArray[j].push([ i, gcCollectionTimes[j][rrdPtr] ]);
            }
            rrdPtr = (rrdPtr + 1) % sysLoad.length;
        }

        $.plot($('#system-load-chart'), [ {
            label : 'System Load',
            data : sysLoadArray
        } ], options);

        var gcNames = result['gc-names'];
        for ( var i = 0; i < gcCollectionCounts.length; ++i) {
            $('#gc-collection-count-' + i).addClass("time-chart");
            $.plot($('#gc-collection-count-' + i), [ {
                label : gcNames[i] + ' Collection Count',
                data : gcCollectionCountsArray[i]
            } ], options);

            $('#gc-collection-time-' + i).addClass("time-chart");
            $.plot($('#gc-collection-time-' + i), [ {
                label : gcNames[i] + ' Collection Time (ms)',
                data : gcCollectionTimesArray[i]
            } ], options);
        }

        $.plot($('#heap-usage-chart'), [ {
            label : 'Heap Usage Initial Size (MB)',
            data : heapUsageInitSizesArray
        }, {
            label : 'Heap Usage Used Size (MB)',
            data : heapUsageUsedSizesArray
        }, {
            label : 'Heap Usage Committed Size (MB)',
            data : heapUsageCommittedSizesArray
        }, {
            label : 'Heap Usage Max Size (MB)',
            data : heapUsageMaxSizesArray
        } ], options);

        $.plot($('#nonheap-usage-chart'), [ {
            label : 'Non-Heap Usage Initial Size (MB)',
            data : nonheapUsageInitSizesArray
        }, {
            label : 'Non-Heap Usage Used Size (MB)',
            data : nonheapUsageUsedSizesArray
        }, {
            label : 'Non-Heap Usage Committed Size (MB)',
            data : nonheapUsageCommittedSizesArray
        }, {
            label : 'Non-Heap Usage Max Size (MB)',
            data : nonheapUsageMaxSizesArray
        } ], options);

        $.plot($('#thread-chart'), [ {
            label : 'Thread Count',
            data : threadCountsArray
        }, {
            label : 'Peak Thread Count',
            data : peakThreadCountsArray
        } ], options);
    }

    function fetchData() {
        $.ajax({
            url : '/rest/nodes/' + $.getURLParam('node-id'),
            method : 'GET',
            dataType : 'json',
            success : onDataReceived
        });

        setTimeout(fetchData, 10000);
    }

    fetchData();
});