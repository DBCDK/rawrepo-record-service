/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

public class EnqueueAgencyRequestDTO {
    String worker;
    Integer selectAgencyId;
    Integer enqueueAgencyId;

    public String getWorker() {
        return worker;
    }

    public void setWorker(String worker) {
        this.worker = worker;
    }

    public Integer getSelectAgencyId() {
        return selectAgencyId;
    }

    public void setSelectAgencyId(Integer selectAgencyId) {
        this.selectAgencyId = selectAgencyId;
    }

    public Integer getEnqueueAgencyId() {
        return enqueueAgencyId;
    }

    public void setEnqueueAgencyId(Integer enqueueAgencyId) {
        this.enqueueAgencyId = enqueueAgencyId;
    }
}
