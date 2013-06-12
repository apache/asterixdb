/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.exceptions;

import edu.uci.ics.asterix.common.transactions.ITransactionContext;

/**
 * Represents an exception related to an unexpected behavior that prevents the
 * system from supporting ACID guarantees. The exception contains the
 * ITransactionContext but it may not always be set. For example, an
 * ACIDException encountered during crash recovery shall not have a transaction
 * context as recovery does not happen as part of a transaction.
 */
public class ACIDException extends Exception {

    /**
	 * 
	 */
    private static final long serialVersionUID = -8855848112541877323L;
    private ITransactionContext txnContext;

    public ITransactionContext getTxnContext() {
        return txnContext;
    }

    public void setTxnContext(ITransactionContext txnContext) {
        this.txnContext = txnContext;
    }

    public ACIDException(ITransactionContext txnContext, String message) {
        super(message);
        this.txnContext = txnContext;
    }

    public ACIDException(ITransactionContext txnContext, String message, Throwable cause) {
        super(message, cause);
        this.txnContext = txnContext;
    }

    public ACIDException(String message, Throwable cause) {
        super(message, cause);
    }

    public ACIDException(String message) {
        super(message);
    }
    
    public ACIDException(Throwable cause) {
        super(cause);
    }

}
