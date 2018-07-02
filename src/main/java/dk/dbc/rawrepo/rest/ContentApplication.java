/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.rest;

import dk.dbc.rawrepo.service.ContentJSONService;
import dk.dbc.rawrepo.service.ContentLineService;
import dk.dbc.rawrepo.service.ContentXMLService;
import dk.dbc.rawrepo.service.RecordService;
import dk.dbc.rawrepo.writer.MarcRecordCollectionXMLMessageBodyWriter;
import dk.dbc.rawrepo.writer.MarcRecordXMLMessageBodyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@ApplicationPath("/")
public class ContentApplication extends Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContentApplication.class);

    @Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> classes = new HashSet<>();
        classes.add(StatusBean.class);

        classes.add(ContentXMLService.class);
        classes.add(ContentLineService.class);
        classes.add(ContentJSONService.class);
        classes.add(RecordService.class);

        classes.add(MarcRecordXMLMessageBodyWriter.class);
        classes.add(MarcRecordCollectionXMLMessageBodyWriter.class);

        for (Class<?> clazz : classes) {
            LOGGER.info("Registered {} resource", clazz.getName());
        }
        return classes;
    }
}
