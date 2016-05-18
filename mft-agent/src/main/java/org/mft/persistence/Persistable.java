package org.mft.persistence;

import java.io.IOException;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public interface Persistable {
   void accept(Persister persister) throws IOException;
}
