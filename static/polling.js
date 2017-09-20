$(function() {
    triggerPolling();
})

function triggerPolling(){
    $('#submissions > tbody > tr').each(function(){
        var row = $(this);
        pollRow(row);
    });
}

function pollRow(row){
    var POLL_INTERVAL = 5000; //in milliseconds
    var LAST_UPDATE_INTERVAL = 1; //in minutes, added to capture any updates that happened in the last 1 minute

    var rowData = row.data();
    var url = rowData.url;
    var date = new Date(rowData.date);
    date.setMinutes(date.getMinutes() - LAST_UPDATE_INTERVAL);

    var submissionId = url.split("/").pop();

    $.ajax({
        url: '/submission/'+ submissionId + '/'+ date.toUTCString(),
        type: 'GET',
        dataType: 'html',
        success: function(response){
            if(response){
                row.html(response);
                var now = new Date();
                now.setMinutes(now.getMinutes() - LAST_UPDATE_INTERVAL);

                row.data('date', now.toUTCString());
                console.log('updated row' + now.toUTCString());
            }
        },
        complete: function(data){
            setTimeout(function(){
                pollRow(row);
            }, POLL_INTERVAL)
        },
    });
}
