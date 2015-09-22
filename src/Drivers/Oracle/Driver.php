<?php
/**
 * O2DB
 *
 * An open source PHP database engine driver for PHP 5.4 or newer
 *
 * This content is released under the MIT License (MIT)
 *
 * Copyright (c) 2014, PT. Lingkar Kreasi (Circle Creative).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @package        O2System
 * @author         Steeven Andrian Salim
 * @copyright      Copyright (c) 2005 - 2014, PT. Lingkar Kreasi (Circle Creative).
 * @license        http://circle-creative.com/products/o2db/license.html
 * @license        http://opensource.org/licenses/MIT   MIT License
 * @link           http://circle-creative.com/products/o2db.html
 * @filesource
 */
// ------------------------------------------------------------------------

namespace O2System\O2DB\Drivers\Oracle;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * Oracle Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * Statement ID
     *
     * @access  public
     * @type    resource
     */
    public $id_statement;

    /**
     * Cursor ID
     *
     * @access  public
     * @type    resource
     */
    public $id_cursor;

    /**
     * Commit mode flag
     *
     * @access  public
     * @type    int
     */
    public $commit_mode = OCI_COMMIT_ON_SUCCESS;

    /**
     * Limit used flag
     *
     * If we use LIMIT, we'll add a field that will
     * throw off num_fields later.
     *
     * @access  public
     * @type    bool
     */
    public $limit_used;

    // --------------------------------------------------------------------

    /**
     * List of reserved identifiers
     *
     * Identifiers that must NOT be escaped.
     *
     * @access  public
     * @type    string[]
     */
    protected $_reserved_identifiers = array( '*', 'rownum' );

    /**
     * ORDER BY random keyword
     *
     * @access  public
     * @type    array
     */
    protected $_random_keywords = array( 'ASC', 'ASC' ); // not currently supported

    /**
     * COUNT string
     *
     * @used-by    O2System\Libraries\DB_driver::count_all()
     * @used-by    O2System\Libraries\DB_query_builder::count_all_results()
     *
     * @access     public
     * @type    string
     */
    protected $_count_string = 'SELECT COUNT(1) AS ';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param   array $params
     *
     * @access public
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        $valid_dsns = array(
            'tns' => '/^\(DESCRIPTION=(\(.+\)){2,}\)$/', // TNS
            // Easy Connect string (Oracle 10g+)
            'ec'  => '/^(\/\/)?[a-z0-9.:_-]+(:[1-9][0-9]{0,4})?(\/[a-z0-9$_]+)?(:[^\/])?(\/[a-z0-9$_]+)?$/i',
            'in'  => '/^[a-z0-9$_]+$/i' // Instance name (defined in tnsnames.ora)
        );

        /* Space characters don't have any effect when actually
         * connecting, but can be a hassle while validating the DSN.
         */
        $this->dsn = str_replace( array( "\n", "\r", "\t", ' ' ), '', $this->dsn );

        if( $this->dsn !== '' )
        {
            foreach( $valid_dsns as $regexp )
            {
                if( preg_match( $regexp, $this->dsn ) )
                {
                    return;
                }
            }
        }

        // Legacy support for TNS in the hostname configuration field
        $this->hostname = str_replace( array( "\n", "\r", "\t", ' ' ), '', $this->hostname );
        if( preg_match( $valid_dsns[ 'tns' ], $this->hostname ) )
        {
            $this->dsn = $this->hostname;

            return;
        }
        elseif( $this->hostname !== '' && strpos( $this->hostname, '/' ) === FALSE && strpos( $this->hostname, ':' ) === FALSE
                && ( ( ! empty( $this->port ) && ctype_digit( $this->port ) ) OR $this->database !== '' )
        )
        {
            /* If the hostname field isn't empty, doesn't contain
             * ':' and/or '/' and if port and/or database aren't
             * empty, then the hostname field is most likely indeed
             * just a hostname. Therefore we'll try and build an
             * Easy Connect string from these 3 settings, assuming
             * that the database field is a service name.
             */
            $this->dsn = $this->hostname
                         . ( ( ! empty( $this->port ) && ctype_digit( $this->port ) ) ? ':' . $this->port : '' )
                         . ( $this->database !== '' ? '/' . ltrim( $this->database, '/' ) : '' );

            if( preg_match( $valid_dsns[ 'ec' ], $this->dsn ) )
            {
                return;
            }
        }

        /* At this point, we can only try and validate the hostname and
         * database fields separately as DSNs.
         */
        if( preg_match( $valid_dsns[ 'ec' ], $this->hostname ) OR preg_match( $valid_dsns[ 'in' ], $this->hostname ) )
        {
            $this->dsn = $this->hostname;

            return;
        }

        $this->database = str_replace( array( "\n", "\r", "\t", ' ' ), '', $this->database );
        foreach( $valid_dsns as $regexp )
        {
            if( preg_match( $regexp, $this->database ) )
            {
                return;
            }
        }

        /* Well - OK, an empty string should work as well.
         * PHP will try to use environment variables to
         * determine which Oracle instance to connect to.
         */
        $this->dsn = '';
    }

    // --------------------------------------------------------------------

    /**
     * Non-persistent database connection
     *
     * @param   bool $persistent
     *
     * @access  public
     * @return  resource
     */
    public function connect( $persistent = FALSE )
    {
        $func = ( $persistent === TRUE ) ? 'oci_pconnect' : 'oci_connect';

        return empty( $this->charset )
            ? $func( $this->username, $this->password, $this->dsn )
            : $func( $this->username, $this->password, $this->dsn, $this->charset );
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * @access  public
     * @return  string
     */
    public function version()
    {
        if( isset( $this->data_cache[ 'version' ] ) )
        {
            return $this->data_cache[ 'version' ];
        }

        if( ! $this->id_connection OR ( $version = oci_server_version( $this->id_connection ) ) === FALSE )
        {
            return FALSE;
        }

        return $this->data_cache[ 'version' ] = $version;
    }

    // --------------------------------------------------------------------

    /**
     * Get cursor. Returns a cursor from the database
     *
     * @access  public
     * @return  resource
     */
    public function get_cursor()
    {
        return $this->id_cursor = oci_new_cursor( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Stored Procedure.  Executes a stored procedure
     *
     * @example
     * $params array keys
     *
     * KEY      OPTIONAL    NOTES
     * name     no          the name of the parameter should be in :<param_name> format
     * value    no          the value of the parameter.  If this is an OUT or IN OUT parameter,
     *                      this should be a reference to a variable
     * type     yes         the type of the parameter
     * length   yes         the max size of the parameter
     *
     * @param    string    package name in which the stored procedure is in
     * @param    string    stored procedure name to execute
     * @param    array     parameters
     *
     * @access  public
     * @return  mixed
     * @throws  \Exception
     */
    public function stored_procedure( $package, $procedure, $params )
    {
        if( $package === '' OR $procedure === '' OR ! is_array( $params ) )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'Invalid query: ' . $package . '.' . $procedure );
            }

            return FALSE;
        }

        // build the query string
        $sql = 'BEGIN ' . $package . '.' . $procedure . '(';

        $have_cursor = FALSE;
        foreach( $params as $param )
        {
            $sql .= $param[ 'name' ] . ',';

            if( isset( $param[ 'type' ] ) && $param[ 'type' ] === OCI_B_CURSOR )
            {
                $have_cursor = TRUE;
            }
        }
        $sql = trim( $sql, ',' ) . '); END;';

        $this->id_statement = FALSE;
        $this->_set_statement_id( $sql );
        $this->_bind_params( $params );

        return $this->query( $sql, FALSE, $have_cursor );
    }

    // --------------------------------------------------------------------

    /**
     * Bind parameters
     *
     * @param   array   $params
     *
     * @access  protected
     * @return  void
     */
    protected function _bind_params( $params )
    {
        if( ! is_array( $params ) OR ! is_resource( $this->id_statement ) )
        {
            return;
        }

        foreach( $params as $param )
        {
            foreach( array( 'name', 'value', 'type', 'length' ) as $val )
            {
                if( ! isset( $param[ $val ] ) )
                {
                    $param[ $val ] = '';
                }
            }

            oci_bind_by_name( $this->id_statement, $param[ 'name' ], $param[ 'value' ], $param[ 'length' ], $param[ 'type' ] );
        }
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @param   bool $test_mode
     *
     * @access  public
     * @return  bool
     */
    public function trans_begin( $test_mode = FALSE )
    {
        if( ! $this->trans_enabled )
        {
            return TRUE;
        }

        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        // Reset the transaction failure flag.
        // If the $test_mode flag is set to TRUE transactions will be rolled back
        // even if the queries produce a successful result.
        $this->_trans_failure = ( $test_mode === TRUE );

        $this->commit_mode = is_php( '5.3.2' ) ? OCI_NO_AUTO_COMMIT : OCI_DEFAULT;

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @access  public
     * @return  bool
     */
    public function trans_commit()
    {
        if( ! $this->trans_enabled )
        {
            return TRUE;
        }

        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        $this->commit_mode = OCI_COMMIT_ON_SUCCESS;

        return oci_commit( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @access  public
     * @return  bool
     */
    public function trans_rollback()
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        $this->commit_mode = OCI_COMMIT_ON_SUCCESS;

        return oci_rollback( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * @access  public
     * @return  int
     */
    public function affected_rows()
    {
        return oci_num_rows( $this->id_statement );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access  public
     * @return  int
     * @throws  \Exception
     */
    public function insert_id()
    {
        // not supported in oracle
        if( $this->debug_enabled )
        {
            throw new \Exception( 'This feature is not available for the database you are using.' );
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param   string $table
     *
     * @access  public
     * @return  array
     */
    public function field_data( $table )
    {
        if( strpos( $table, '.' ) !== FALSE )
        {
            sscanf( $table, '%[^.].%s', $owner, $table );
        }
        else
        {
            $owner = $this->username;
        }

        $sql = 'SELECT COLUMN_NAME, DATA_TYPE, CHAR_LENGTH, DATA_PRECISION, DATA_LENGTH, DATA_DEFAULT, NULLABLE
			FROM ALL_TAB_COLUMNS
			WHERE UPPER(OWNER) = ' . $this->escape( strtoupper( $owner ) ) . '
				&& UPPER(TABLE_NAME) = ' . $this->escape( strtoupper( $table ) );

        if( ( $query = $this->query( $sql ) ) === FALSE )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $data = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $query[ $i ]->COLUMN_NAME;
            $data[ $i ]->type = $query[ $i ]->DATA_TYPE;

            $length = ( $query[ $i ]->CHAR_LENGTH > 0 )
                ? $query[ $i ]->CHAR_LENGTH : $query[ $i ]->DATA_PRECISION;
            if( $length === NULL )
            {
                $length = $query[ $i ]->DATA_LENGTH;
            }
            $data[ $i ]->max_length = $length;

            $default = $query[ $i ]->DATA_DEFAULT;
            if( $default === NULL && $query[ $i ]->NULLABLE === 'N' )
            {
                $default = '';
            }
            $data[ $i ]->default = $query[ $i ]->COLUMN_DEFAULT;
        }

        return $data;
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occured.
     *
     * @access  public
     * @return  array
     */
    public function error()
    {
        /* oci_error() returns an array that already contains the
         * 'code' and 'message' keys, so we can just return it.
         */
        if( is_resource( $this->id_cursor ) )
        {
            return oci_error( $this->id_cursor );
        }
        elseif( is_resource( $this->id_statement ) )
        {
            return oci_error( $this->id_statement );
        }
        elseif( is_resource( $this->id_connection ) )
        {
            return oci_error( $this->id_connection );
        }

        return oci_error();
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param    string $sql an SQL query
     *
     * @access  protected
     * @return  resource
     */
    protected function _execute( $sql )
    {
        /* Oracle must parse the query before it is run. All of the actions with
         * the query are based on the statement id returned by oci_parse().
         */
        $this->id_statement = FALSE;
        $this->_set_statement_id( $sql );
        oci_set_prefetch( $this->id_statement, 1000 );

        return oci_execute( $this->id_statement, $this->commit_mode );
    }

    // --------------------------------------------------------------------

    /**
     * Generate a statement ID
     *
     * @param   string $sql An SQL query
     *
     * @access  public
     * @return  void
     */
    protected function _set_statement_id( $sql )
    {
        if( ! is_resource( $this->id_statement ) )
        {
            $this->id_statement = oci_parse( $this->id_connection, $sql );
        }
    }

    // --------------------------------------------------------------------

    /**
     * Show table query
     *
     * Generates a platform-specific query string so that the table names can be fetched
     *
     * @param   bool $prefix_limit
     *
     * @access  protected
     * @return  string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = 'SELECT "TABLE_NAME" FROM "ALL_TABLES"';

        if( $prefix_limit !== FALSE && $this->prefix_table !== '' )
        {
            return $sql . ' WHERE "TABLE_NAME" LIKE \'' . $this->escape_like_string( $this->prefix_table ) . "%' "
                   . sprintf( $this->_like_escape_string, $this->_like_escape_character );
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Show column query
     *
     * Generates a platform-specific query string so that the column names can be fetched
     *
     * @param   string $table
     *
     * @access  protected
     * @return  string
     */
    protected function _list_columns( $table = '' )
    {
        if( strpos( $table, '.' ) !== FALSE )
        {
            sscanf( $table, '%[^.].%s', $owner, $table );
        }
        else
        {
            $owner = $this->username;
        }

        return 'SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS
			WHERE UPPER(OWNER) = ' . $this->escape( strtoupper( $owner ) ) . '
				&& UPPER(TABLE_NAME) = ' . $this->escape( strtoupper( $table ) );
    }

    // --------------------------------------------------------------------

    /**
     * Insert batch statement
     *
     * Generates a platform-specific insert string from the supplied data
     *
     * @param   string $table  Table name
     * @param   array  $keys   INSERT keys
     * @param   array  $values INSERT values
     *
     * @access  protected
     * @return  string
     */
    protected function _insert_batch( $table, $keys, $values )
    {
        $keys = implode( ', ', $keys );
        $sql = "INSERT ALL\n";

        for( $i = 0, $c = count( $values ); $i < $c; $i++ )
        {
            $sql .= '	INTO ' . $table . ' (' . $keys . ') VALUES ' . $values[ $i ] . "\n";
        }

        return $sql . 'SELECT * FROM dual';
    }

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the TRUNCATE statement,
     * then this method maps to 'DELETE FROM table'
     *
     * @param   string $table
     *
     * @access  protected
     * @return  string
     */
    protected function _truncate( $table )
    {
        return 'TRUNCATE TABLE ' . $table;
    }

    // --------------------------------------------------------------------

    /**
     * Delete statement
     *
     * Generates a platform-specific delete string from the supplied data
     *
     * @param   string $table
     *
     * @access  protected
     * @return  string
     */
    protected function _delete( $table )
    {
        if( $this->_limit )
        {
            $this->where( 'rownum <= ', $this->_limit, FALSE );
            $this->_limit = FALSE;
        }

        return parent::_delete( $table );
    }

    // --------------------------------------------------------------------

    /**
     * LIMIT
     *
     * Generates a platform-specific LIMIT clause
     *
     * @param   string $sql SQL Query
     *
     * @access  protected
     * @return  string
     */
    protected function _limit( $sql )
    {
        $this->limit_used = TRUE;

        return 'SELECT * FROM (SELECT inner_query.*, rownum rnum FROM (' . $sql . ') inner_query WHERE rownum < ' . ( $this->_offset + $this->_limit + 1 ) . ')'
               . ( $this->_offset ? ' WHERE rnum >= ' . ( $this->_offset + 1 ) : '' );
    }

    // --------------------------------------------------------------------

    /**
     * Close DB Connection
     *
     * @access  protected
     * @return  void
     */
    protected function _close()
    {
        oci_close( $this->id_connection );
    }

}
