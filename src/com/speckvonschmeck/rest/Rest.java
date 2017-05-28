package com.speckvonschmeck.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.speckvonschmeck.models.Spectrum;


@Path("spectra")
public class Rest {
	
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response serverinfo(Spectrum[] spectrum) {
		System.out.println(spectrum[0].toString());
		return Response.ok().build();
	}
}
