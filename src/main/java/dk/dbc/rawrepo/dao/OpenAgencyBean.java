package dk.dbc.rawrepo.dao;

import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.common.ApplicationVariables;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;

@Stateless
public class OpenAgencyBean {

    private static final XLogger LOGGER = XLoggerFactory.getXLogger(OpenAgencyBean.class);

    private OpenAgencyServiceFromURL service;

    @EJB
    private ApplicationVariables environmentVariables;

    @PostConstruct
    public void postConstruct() {
        LOGGER.entry();

        if (!System.getenv().containsKey(ApplicationConstants.OPENAGENCY_URL)) {
            throw new RuntimeException("OPENAGENCY_URL must have a value");
        }

        try {
            OpenAgencyServiceFromURL.Builder builder = OpenAgencyServiceFromURL.builder();
            builder = builder.
                    connectTimeout(Integer.parseInt(environmentVariables.getenv().getOrDefault(
                            ApplicationConstants.OPENAGENCY_CONNECT_TIMEOUT,
                            ApplicationConstants.OPENAGENCY_CONNECT_TIMEOUT_DEFAULT))).
                    requestTimeout(Integer.parseInt(environmentVariables.getenv().getOrDefault(
                            ApplicationConstants.OPENAGENCY_REQUEST_TIMEOUT,
                            ApplicationConstants.OPENAGENCY_REQUEST_TIMEOUT_DEFAULT))).
                    setCacheAge(Integer.parseInt(environmentVariables.getenv().getOrDefault(
                            ApplicationConstants.OPENAGENCY_CACHE_AGE,
                            ApplicationConstants.OPENAGENCY_CACHE_AGE_DEFAULT)));

            service = builder.build(environmentVariables.getenv().get(ApplicationConstants.OPENAGENCY_URL));
        } finally {
            LOGGER.exit();
        }

    }

    public OpenAgencyServiceFromURL getService() {
        return this.service;
    }

}
