package dk.dbc.rawrepo.dao;

import dk.dbc.util.StopwatchInterceptor;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class HoldingsItemsBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(HoldingsItemsBean.class);

    @Resource(lookup = "jdbc/holdings")
    private DataSource dataSource;

    public HashMap<String, String> getRecordIdsWithHolding(int agencyId) throws SQLException {
        HashMap<String, String> res = new HashMap<>();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("SELECT DISTINCT(bibliographicrecordid) FROM holdingsitemscollection WHERE agencyid=?")) {
            stmt.setInt(1, agencyId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String bibliographicRecordId = resultSet.getString(1);
                    res.put(bibliographicRecordId, "holdings");
                }
            }
        }

        LOGGER.info("Found {} records in holdingsitemscollection for agencyId {}", res.size(), agencyId);

        return res;
    }

}
