package dk.dbc.rawrepo.rest;

import dk.dbc.rawrepo.dump.DumpService;
import dk.dbc.rawrepo.service.AgencyService;
import dk.dbc.rawrepo.service.LandingPage;
import dk.dbc.rawrepo.service.MarcConverterService;
import dk.dbc.rawrepo.service.QueueService;
import dk.dbc.rawrepo.service.RecordCollectionService;
import dk.dbc.rawrepo.service.RecordService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@ApplicationPath("/")
public class RawRepoRecordApplication extends Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(RawRepoRecordApplication.class);

    @Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> classes = new HashSet<>();
        classes.add(StatusBean.class);
        classes.add(RecordService.class);
        classes.add(RecordCollectionService.class);
        classes.add(AgencyService.class);
        classes.add(DumpService.class);
        classes.add(LandingPage.class);
        classes.add(QueueService.class);
        classes.add(MarcConverterService.class);

        for (Class<?> clazz : classes) {
            LOGGER.info("Registered {} resource", clazz.getName());
        }
        return classes;
    }
}
