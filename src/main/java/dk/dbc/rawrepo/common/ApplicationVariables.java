package dk.dbc.rawrepo.common;

import javax.ejb.Stateless;
import java.util.Map;

@Stateless
public class ApplicationVariables {

    public String getenv(String key) {
        return System.getenv(key);
    }

    public Map<String, String> getenv() {
        return System.getenv();
    }

}
