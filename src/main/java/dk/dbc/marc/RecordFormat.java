package dk.dbc.marc;

/**
 *
 * This class is copied from the mconv project. For some reason importing dk.dbc.mconv dependency breaks the project.
 * <p>As soon as mconv is imported the following exception occurs:</p>
 * <pre>
 * Caused by: javax.ws.rs.ProcessingException: A MultiException has 2 exceptions.  They are:
 * 1. java.util.ServiceConfigurationError: com.fasterxml.jackson.databind.Module: com.fasterxml.jackson.datatype.jsr310.JSR310Module not a subtype
 * 2. java.lang.IllegalStateException: Unable to perform operation: create on org.glassfish.jersey.jackson.internal.DefaultJacksonJaxbJsonProvider
 *
 * 	at dk.dbc.httpclient.HttpClient.execute(HttpClient.java:144)
 * 	at dk.dbc.httpclient.FailSafeHttpClient.lambda$execute$1(FailSafeHttpClient.java:65)
 * 	at net.jodah.failsafe.Functions.lambda$get$0(Functions.java:48)
 * 	at net.jodah.failsafe.RetryPolicyExecutor.lambda$supply$0(RetryPolicyExecutor.java:62)
 * 	at net.jodah.failsafe.Execution.executeSync(Execution.java:129)
 * 	at net.jodah.failsafe.FailsafeExecutor.call(FailsafeExecutor.java:376)
 * 	at net.jodah.failsafe.FailsafeExecutor.get(FailsafeExecutor.java:67)
 * 	at dk.dbc.httpclient.FailSafeHttpClient.execute(FailSafeHttpClient.java:65)
 * 	at dk.dbc.httpclient.HttpRequest.execute(HttpRequest.java:31)
 * 	at dk.dbc.vipcore.VipCoreConnector.postRequest(VipCoreConnector.java:123)
 * 	at dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector.postLibraryRulesRequest(VipCoreLibraryRulesConnector.java:180)
 * 	at dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector.hasFeature(VipCoreLibraryRulesConnector.java:137)
 * 	at dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector.hasFeature(VipCoreLibraryRulesConnector.java:133)
 * 	at dk.dbc.rawrepo.RelationHintsVipCore.usesCommonAgency(RelationHintsVipCore.java:28)
 * 	at dk.dbc.rawrepo.RecordRelationsBean.findParentRelationAgency(RecordRelationsBean.java:79)
 * 	at dk.dbc.rawrepo.RecordRelationsBean.parentIsActive(RecordRelationsBean.java:213)
 * 	</pre>
 *
 * 	That problem should be fixed and this class removed. The namespace is the same here as in mconv, so no other code changes should be necessary.
 */
// TODO Remove class and instead import from dk.dbc.mconv jar
public enum RecordFormat {
    LINE,
    LINE_CONCAT,
    MARCXCHANGE,
    ISO,
    JSONL,
}
