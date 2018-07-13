/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.interceptor;

import javax.ws.rs.NameBinding;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/*
    Inspired by https://dzone.com/articles/how-compress-responses-java
 */
@NameBinding
@Retention(RetentionPolicy.RUNTIME)
public @interface Compress {}
