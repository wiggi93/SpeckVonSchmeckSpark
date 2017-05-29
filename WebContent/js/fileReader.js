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
//	var spectra = [];
	while(contents.indexOf("BEGIN IONS") >- 1){
		var spectrum = new Spectrum();
		var meta = new Meta();
       
		spectrum.data = [];
        
	    contents = contents.substring(contents.indexOf("\n")+1, contents.length);
	    meta.title = contents.substring(contents.indexOf("TITLE")+6,contents.indexOf("\n",contents.indexOf("TITLE")));
	    contents = contents.substring(contents.indexOf("\n")+1, contents.length);
	    meta.pepmass = contents.substring(contents.indexOf("PEPMASS")+8,contents.indexOf("\n",contents.indexOf("PEPMASS")));
	    contents = contents.substring(contents.indexOf("\n")+1, contents.length);
	    meta.charge = contents.substring(contents.indexOf("CHARGE")+7,contents.indexOf("\n",contents.indexOf("CHARGE")));
        contents = contents.substring(contents.indexOf("\n")+1, contents.length);
        meta.rtInSeconds = contents.substring(contents.indexOf("RTINSECONDS")+12,contents.indexOf("\n",contents.indexOf("RTINSECONDS")));
        contents = contents.substring(contents.indexOf("\n")+1, contents.length);
        meta.scans = contents.substring(contents.indexOf("SCANS")+6,contents.indexOf("\n",contents.indexOf("SCANS")));
        contents = contents.substring(contents.indexOf("\n")+1, contents.length);

        spectrum.meta = meta;

        while(contents.indexOf("END IONS")!=0){
        	let data = new Data();
            data.x = contents.substring(0,contents.indexOf(" "));
            data.y = contents.substring(contents.indexOf(" ")+1, contents.indexOf("\n"));
            spectrum.data.push(data);
            contents = contents.substring(contents.indexOf("\n")+1, contents.length);
        }
        
        uploadJson(JSON.stringify(spectrum));
//        spectra.push(spectrum);
        contents=contents.substring(contents.indexOf("BEGIN IONS"), contents.length);

    }

    
}

function uploadJson(json){
	console.log(json);
	$.ajax({
    	type:"POST",
    	contentType: "application/json",
    	url:"http://localhost:8080/rest/spectra",
    	data:json,
    	sucess: function(msg){
    		alert("nice");
    	}
    });
}


