/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.rawrepo.dao.RawRepoBean;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

@Startup
@Singleton
public class InitializerBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(InitializerBean.class);

    /**
     * The purpose of RECORD_SERVICE_URL is to allow record service to insert its own URL in the configurations table.
     */
    @Inject
    @ConfigProperty(name = "RECORD_SERVICE_URL", defaultValue = "IGNORE")
    private String RECORD_SERVICE_URL;

    @EJB
    private RawRepoBean rawRepoBean;

    @PostConstruct
    public void postConstruct() {
        try {
            LOGGER.info("Running postConstruct...");

            if (!RECORD_SERVICE_URL.equals("IGNORE")) {
                rawRepoBean.setConfigurations("RECORD_SERVICE_URL", RECORD_SERVICE_URL);
                LOGGER.info("Configuration table updated with RECORD_SERVICE_URL = {}", RECORD_SERVICE_URL);
            }

            LOGGER.info("postConstruct done!");
        } catch (Exception ex) {
            LOGGER.error("Caught exception", ex);
            throw new RuntimeException(ex);
        }
    }
}
