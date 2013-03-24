$(function() {
    $('#tabs').tabs();

    var options = {
        lines : {
            show : true,
            fill : true
        },
        points : {
            show : false
        },
        xaxis : {
            tickDecimals : 0,
            tickSize : 0
        }
    };

    function computeRate(array, rrdPtr) {
        return (array[(rrdPtr + 1) % array.length] - array[rrdPtr]) / 10;
    }

    function onDataReceived(data) {
        var result = data.result;
        $('#node-id')[0].innerHTML = result['node-id'];
        $('#arch')[0].innerHTML = result['arch'];
        $('#os-name')[0].innerHTML = result['os-name'];
        $('#os-version')[0].innerHTML = result['os-version'];
        $('#num-processors')[0].innerHTML = result['num-processors'];
        $('#vm-name')[0].innerHTML = result['vm-name'];
        $('#vm-version')[0].innerHTML = result['vm-version'];
        $('#vm-vendor')[0].innerHTML = result['vm-vendor'];
        $('#classpath')[0].innerHTML = result['classpath'];
        $('#library-path')[0].innerHTML = result['library-path'];
        $('#boot-classpath')[0].innerHTML = result['boot-classpath'];
        var argsHTML = "";
        var args = result['input-arguments'];
        for ( var i = 0; i < args.length; ++i) {
            if (argsHTML != "") {
                argsHTML += "<br/>";
            }
            argsHTML += "<span>" + args[i] + "</span>";
        }
        $('#input-arguments')[0].innerHTML = argsHTML;
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
        var netPayloadBytesRead = result['net-payload-bytes-read'];
        var netPayloadBytesWritten = result['net-payload-bytes-written'];
        var netSignalingBytesRead = result['net-signaling-bytes-read'];
        var netSignalingBytesWritten = result['net-signaling-bytes-written'];
        var datasetNetPayloadBytesRead = result['dataset-net-payload-bytes-read'];
        var datasetNetPayloadBytesWritten = result['dataset-net-payload-bytes-written'];
        var datasetNetSignalingBytesRead = result['dataset-net-signaling-bytes-read'];
        var datasetNetSignalingBytesWritten = result['dataset-net-signaling-bytes-written'];
        var ipcMessagesSent = result['ipc-messages-sent'];
        var ipcMessageBytesSent = result['ipc-message-bytes-sent'];
        var ipcMessagesReceived = result['ipc-messages-received'];
        var ipcMessageBytesReceived = result['ipc-message-bytes-received'];

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
        var netPayloadReadBWArray = [];
        var netPayloadWriteBWArray = [];
        var netSignalingReadBWArray = [];
        var netSignalingWriteBWArray = [];
        var ipcMessageSendRateArray = [];
        var ipcMessageBytesSendRateArray = [];
        var ipcMessageReceiveRateArray = [];
        var ipcMessageBytesReceiveRateArray = [];
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
            if (i < sysLoad.length - 1) {
                netPayloadReadBWArray.push([ i, computeRate(netPayloadBytesRead, rrdPtr) ]);
                netPayloadReadBWArray.push([ i, computeRate(datasetNetPayloadBytesRead, rrdPtr) ]);
                netPayloadWriteBWArray.push([ i, computeRate(netPayloadBytesWritten, rrdPtr) ]);
                netPayloadWriteBWArray.push([ i, computeRate(datasetNetPayloadBytesWritten, rrdPtr) ]);
                netSignalingReadBWArray.push([ i, computeRate(netSignalingBytesRead, rrdPtr) ]);
                netSignalingReadBWArray.push([ i, computeRate(datasetNetSignalingBytesRead, rrdPtr) ]);
                netSignalingWriteBWArray.push([ i, computeRate(netSignalingBytesWritten, rrdPtr) ]);
                netSignalingWriteBWArray.push([ i, computeRate(netSignalingBytesWritten, rrdPtr) ]);
                ipcMessageSendRateArray.push([ i, computeRate(ipcMessagesSent, rrdPtr) ]);
                ipcMessageBytesSendRateArray.push([ i, computeRate(ipcMessageBytesSent, rrdPtr) ]);
                ipcMessageReceiveRateArray.push([ i, computeRate(ipcMessagesReceived, rrdPtr) ]);
                ipcMessageBytesReceiveRateArray.push([ i, computeRate(ipcMessageBytesReceived, rrdPtr) ]);
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

        $.plot($('#net-payload-bandwidth'), [ {
            label : 'Payload Read Bandwidth (bytes/sec)',
            data : netPayloadReadBWArray
        }, {
            label : 'Payload Write Bandwidth (bytes/sec)',
            data : netPayloadWriteBWArray
        } ], options);

        $.plot($('#net-signaling-bandwidth'), [ {
            label : 'Signaling Read Bandwidth (bytes/sec)',
            data : netSignalingReadBWArray
        }, {
            label : 'Signaling Write Bandwidth (bytes/sec)',
            data : netSignalingWriteBWArray
        } ], options);

        $.plot($('#ipc-messages'), [ {
            label : 'IPC Messages Send Rate (messages/sec)',
            data : ipcMessageSendRateArray
        }, {
            label : 'IPC Messages Receive Rate (messages/sec)',
            data : ipcMessageReceiveRateArray
        } ], options);

        $.plot($('#ipc-message-bytes'), [ {
            label : 'IPC Message Send Bandwidth (bytes/sec)',
            data : ipcMessageBytesSendRateArray
        }, {
            label : 'IPC Message Receive Bandwidth (bytes/sec)',
            data : ipcMessageBytesReceiveRateArray
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
