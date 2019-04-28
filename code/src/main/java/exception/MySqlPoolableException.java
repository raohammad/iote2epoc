package exception;

/**
 * Created by hammadakhan on 11/09/2017.
 */
@SuppressWarnings("serial")
public class MySqlPoolableException extends Exception {
    public MySqlPoolableException(final String msg, Exception e) {
        super(msg, e);
    }
}
