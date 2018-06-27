package dk.dbc.rawrepo.service;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.DanMarc2LineFormatWriter;
import dk.dbc.marc.writer.MarcWriterException;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.nio.charset.Charset;
import java.util.Collection;

@Stateless
@Path("api")
public class ContentLineService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(ContentLineService.class);

    @EJB
    private MarcRecordBean marcRecordBean;

    @GET
    @Path("v2/raw/{agencyid}/{bibliographicrecordid}/line")
    @Produces({MediaType.TEXT_PLAIN})
    public String GetRaw(@PathParam("agencyid") int agencyId,
                         @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        try {
            final MarcRecord record = marcRecordBean.getRawRecord(bibliographicRecordId, agencyId);

            DanMarc2LineFormatWriter writer = new DanMarc2LineFormatWriter();

            return new String(writer.write(record, Charset.forName("UTF-8")));
        } catch (MarcWriterException e) {
            throw new InternalServerErrorException();
        } finally {
            LOGGER.info("v2/raw/{agencyid}/{bibliographicrecordid}/line");
        }
    }

    @GET
    @Path("v2/merged/{agencyid}/{bibliographicrecordid}/line")
    @Produces({MediaType.TEXT_PLAIN})
    public String GetMerged(@PathParam("agencyid") int agencyId,
                            @PathParam("bibliographicrecordid") String bibliographicRecordId,
                            @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                            @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                            @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final MarcRecord record = marcRecordBean.getRecordMergedOrExpanded(bibliographicRecordId, agencyId, false, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            DanMarc2LineFormatWriter writer = new DanMarc2LineFormatWriter();

            return new String(writer.write(record, Charset.forName("UTF-8")));
        } catch (MarcWriterException e) {
            throw new InternalServerErrorException();
        } finally {
            LOGGER.info("v2/raw/{agencyid}/{bibliographicrecordid}/line");
        }
    }

    @GET
    @Path("v2/expanded/{agencyid}/{bibliographicrecordid}/line")
    @Produces({MediaType.TEXT_PLAIN})
    public String GetExpanded(@PathParam("agencyid") int agencyId,
                              @PathParam("bibliographicrecordid") String bibliographicRecordId,
                              @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                              @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                              @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final MarcRecord record = marcRecordBean.getRecordMergedOrExpanded(bibliographicRecordId, agencyId, true, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            DanMarc2LineFormatWriter writer = new DanMarc2LineFormatWriter();

            return new String(writer.write(record, Charset.forName("UTF-8")));
        } catch (MarcWriterException e) {
            throw new InternalServerErrorException();
        } finally {
            LOGGER.info("v2/raw/{agencyid}/{bibliographicrecordid}/line");
        }
    }

    @GET
    @Path("v2/collection/{agencyid}/{bibliographicrecordid}/line")
    @Produces({MediaType.TEXT_PLAIN})
    public String GetCollection(@PathParam("agencyid") int agencyId,
                                @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                                @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final Collection<MarcRecord> collection = marcRecordBean.getRecordCollection(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            final DanMarc2LineFormatWriter writer = new DanMarc2LineFormatWriter();

            final StringBuilder builder = new StringBuilder();
            for (MarcRecord marcRecord : collection) {
                builder.append(new String(writer.write(marcRecord, Charset.forName("UTF-8"))));
                builder.append("\n");
            }

            return builder.toString();
        } catch (MarcWriterException e) {
            throw new InternalServerErrorException();
        } finally {
            LOGGER.info("v2/collection/{agencyid}/{bibliographicrecordid}/line");
        }
    }
}
