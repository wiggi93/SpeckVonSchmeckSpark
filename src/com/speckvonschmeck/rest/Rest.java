package com.speckvonschmeck.rest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.speckvonschmeck.models.Data;
import com.speckvonschmeck.models.Spectrum;


@Path("spectra")
public class Rest {
	
	@SuppressWarnings("unchecked")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response serverinfo(List<Map<String, Object>> jsonObject) {
		List<Spectrum> spectra = new ArrayList<Spectrum>();
		for(int i = 0; i < jsonObject.size(); i++){
			Spectrum spectrum = new Spectrum();
			spectrum.setData((List<Data>) jsonObject.get(i).get("data"));
			spectrum.setMeta((LinkedHashMap<String, String>) jsonObject.get(i).get("meta"));
			spectra.add(spectrum);
		}
		
		for(Spectrum sp : spectra){
			System.out.println("DATA");
			System.out.println(sp.getData());
			System.out.println("META");
			System.out.println(sp.getMeta());
		}
		System.out.println(spectra.size());
		return Response.ok(spectra.size()).build();
	}
}
