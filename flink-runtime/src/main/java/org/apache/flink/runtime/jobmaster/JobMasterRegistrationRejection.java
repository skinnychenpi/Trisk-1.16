package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.registration.RegistrationResponse;

public class JobMasterRegistrationRejection extends RegistrationResponse.Rejection {
    private static final long serialVersionUID = -5763721635090700901L;

    private final String reason;

    public JobMasterRegistrationRejection(String reason) {
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "The JobMaster Registration has been rejected because: " + reason;
    }
}
