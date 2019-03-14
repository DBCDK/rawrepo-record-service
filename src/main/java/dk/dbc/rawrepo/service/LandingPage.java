package dk.dbc.rawrepo.service;

import javax.ejb.Stateless;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Stateless
@Path("/")
public class LandingPage {

    @GET
    public Response welcome() {
        return Response.ok("Welcome to rawrepo-record-service").build();
    }
}
