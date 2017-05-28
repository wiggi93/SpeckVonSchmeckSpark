function readMultipleFiles(evt) {
    //Retrieve all the files from the FileList object
    var files = evt.target.files;

    if (files) {
        for (var i = 0, f; f = files[i]; i++) {
            var r = new FileReader();
            r.onload = (function (f) {
                return function (e) {
                    var contents = e.target.result;
                    parserTeppich(contents);
                };
            })(f);
            r.readAsText(f);
        }
    } else {
     alert("Failed to load files");
    }
}
document.getElementById('openFile').addEventListener('change', readMultipleFiles, false);


function parserTeppich(contents){
    var spectra=[];

   while(contents.indexOf("BEGIN IONS")>-1){
       var spectrum=_spectrum;
       var meta=_meta;
       var data=[];
        
        contents=contents.substring(contents.indexOf("\n")+1, contents.length);
        meta.title=contents.substring(contents.indexOf("TITLE")+6,contents.indexOf("\n",contents.indexOf("TITLE")));
        contents=contents.substring(contents.indexOf("\n")+1, contents.length);
        meta.pepmass=contents.substring(contents.indexOf("PEPMASS")+8,contents.indexOf("\n",contents.indexOf("PEPMASS")));
        contents=contents.substring(contents.indexOf("\n")+1, contents.length);
        meta.charge=contents.substring(contents.indexOf("CHARGE")+7,contents.indexOf("\n",contents.indexOf("CHARGE")));
        contents=contents.substring(contents.indexOf("\n")+1, contents.length);
        meta.rtInSeconds=contents.substring(contents.indexOf("RTINSECONDS")+12,contents.indexOf("\n",contents.indexOf("RTINSECONDS")));
        contents=contents.substring(contents.indexOf("\n")+1, contents.length);
        meta.scans=contents.substring(contents.indexOf("SCANS")+6,contents.indexOf("\n",contents.indexOf("SCANS")));
        contents=contents.substring(contents.indexOf("\n")+1, contents.length);

        spectrum.meta=meta;

        while(contents.indexOf("END IONS")!=0){
            let zeile=[];
            zeile.push(contents.substring(0,contents.indexOf(" ")));
            zeile.push(contents.substring(contents.indexOf(" ")+1, contents.indexOf("\n")));
            data.push(zeile);
            contents=contents.substring(contents.indexOf("\n")+1, contents.length);
        }
        spectrum.data=data;
        spectra.push(spectrum);
        contents=contents.substring(contents.indexOf("BEGIN IONS"), contents.length);

    }

    console.log(spectra);
    

    var jsonspec = JSON.stringify(spectra);
    console.log(jsonspec);

    $.ajax({
    	   type:"POST",
    	   contentType: "application/json",
    	   url:"http://192.168.178.64:8080/rest/spectra",
    	   data:jsonspec,
    	   sucess: function(msg){
    		   alert("nice");
    	   }
    });
}


/*
    $('#openFile').fileupload({
        dataType: 'json',
        add: function (e, data) {
            data.context = $('<button/>').text('Upload')
                .appendTo(document.body)
                .click(function () {
                    data.context = $('<p/>').text('Uploading...').replaceAll($(this));
                    data.submit();
                });
        },
        done: function (e, data) {
            data.context.text('Upload finished.');
        }
    });
*/

