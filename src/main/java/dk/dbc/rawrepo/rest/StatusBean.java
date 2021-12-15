package dk.dbc.rawrepo.rest;

import dk.dbc.serviceutils.ServiceStatus;

import javax.ejb.Stateless;
import javax.ws.rs.Path;

@Stateless
@Path("/api")
public class StatusBean implements ServiceStatus {
}
