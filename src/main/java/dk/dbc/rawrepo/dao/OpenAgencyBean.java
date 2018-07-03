package dk.dbc.rawrepo.dao;

import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.inject.Inject;

@Stateless
public class OpenAgencyBean {

    private static final XLogger LOGGER = XLoggerFactory.getXLogger(OpenAgencyBean.class);

    private OpenAgencyServiceFromURL service;

    @Inject
    @ConfigProperty(name = "OPENAGENCY_CONNECT_TIMEOUT", defaultValue = "60000") // 60 seconds
    private String OPENAGENCY_CONNECT_TIMEOUT;

    @Inject
    @ConfigProperty(name = "OPENAGENCY_REQUEST_TIMEOUT", defaultValue = "180000") // 180 seconds
    private String OPENAGENCY_REQUEST_TIMEOUT;

    @Inject
    @ConfigProperty(name = "OPENAGENCY_CACHE_AGE", defaultValue = "8") // 8 hours
    private String OPENAGENCY_CACHE_AGE;

    @Inject
    @ConfigProperty(name = "OPENAGENCY_URL", defaultValue = "OPENAGENCY_URL not set")
    private String OPENAGENCY_URL;

    @PostConstruct
    public void postConstruct() {
        LOGGER.entry();

        try {

            LOGGER.info("Initializing open agency with the following parameters:");
            LOGGER.info("OPENAGENCY_URL: {}", OPENAGENCY_URL);
            LOGGER.info("OPENAGENCY_CONNECT_TIMEOUT: {}", Integer.parseInt(OPENAGENCY_CONNECT_TIMEOUT));
            LOGGER.info("OPENAGENCY_REQUEST_TIMEOUT: {}", Integer.parseInt(OPENAGENCY_REQUEST_TIMEOUT));
            LOGGER.info("OPENAGENCY_CACHE_AGE: {}", Integer.parseInt(OPENAGENCY_CACHE_AGE));

            OpenAgencyServiceFromURL.Builder builder = OpenAgencyServiceFromURL.builder();
            builder = builder.connectTimeout(Integer.parseInt(OPENAGENCY_CONNECT_TIMEOUT)).
                    requestTimeout(Integer.parseInt(OPENAGENCY_REQUEST_TIMEOUT)).
                    setCacheAge(Integer.parseInt(OPENAGENCY_CACHE_AGE));
            service = builder.build(OPENAGENCY_URL);
        } finally {
            LOGGER.exit();
        }

    }

    public OpenAgencyServiceFromURL getService() {
        return this.service;
    }

}
