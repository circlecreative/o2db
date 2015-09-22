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

namespace O2System\O2DB\Drivers\Postgre;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * Postgre Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * Database schema
     *
     * @access  protected
     * @type    string
     */
    public $schema = 'public';

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @access  protected
     * @type    array
     */
    protected $_random_keywords = array( 'RANDOM()', 'RANDOM()' );

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Creates a DSN string to be used for connect() and db_pconnect()
     *
     * @param   array $params
     *
     * @access  public
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( ! empty( $this->dsn ) )
        {
            return;
        }

        $this->dsn === '' OR $this->dsn = '';

        if( strpos( $this->hostname, '/' ) !== FALSE )
        {
            // If UNIX sockets are used, we shouldn't set a port
            $this->port = '';
        }

        $this->hostname === '' OR $this->dsn = 'host=' . $this->hostname . ' ';

        if( ! empty( $this->port ) && ctype_digit( $this->port ) )
        {
            $this->dsn .= 'port=' . $this->port . ' ';
        }

        if( $this->username !== '' )
        {
            $this->dsn .= 'user=' . $this->username . ' ';

            /* An empty password is valid!
             *
             * $db['password'] = NULL must be done in order to ignore it.
             */
            $this->password === NULL OR $this->dsn .= "password='" . $this->password . "' ";
        }

        $this->database === '' OR $this->dsn .= 'dbname=' . $this->database . ' ';

        /* We don't have these options as elements in our standard configuration
         * array, but they might be set by parse_url() if the configuration was
         * provided via string. Example:
         *
         * postgre://username:password@localhost:5432/database?connect_timeout=5&sslmode=1
         */
        foreach( array( 'connect_timeout', 'options', 'sslmode', 'service' ) as $key )
        {
            if( isset( $this->$key ) && is_string( $this->parameter ) && $this->parameter !== '' )
            {
                $this->dsn .= $key . "='" . $this->parameter . "' ";
            }
        }

        $this->dsn = rtrim( $this->dsn );
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param   bool $persistent
     *
     * @access  public
     * @return  resource
     */
    public function connect( $persistent = FALSE )
    {
        $this->id_connection = ( $persistent === TRUE )
            ? pg_pconnect( $this->dsn )
            : pg_connect( $this->dsn );

        if( $this->id_connection !== FALSE )
        {
            if( $persistent === TRUE
                && pg_connection_status( $this->id_connection ) === PGSQL_CONNECTION_BAD
                && pg_ping( $this->id_connection ) === FALSE
            )
            {
                return FALSE;
            }

            empty( $this->schema ) OR $this->simple_query( 'SET search_path TO ' . $this->schema . ',public' );
        }

        return $this->id_connection;
    }

    // --------------------------------------------------------------------

    /**
     * Reconnect
     *
     * Keep / reestablish the db connection if no queries have been
     * sent for a length of time exceeding the server's idle timeout
     *
     * @access  public
     * @return  void
     */
    public function reconnect()
    {
        if( pg_ping( $this->id_connection ) === FALSE )
        {
            $this->id_connection = FALSE;
        }
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

        if( ! $this->id_connection OR ( $pg_version = pg_version( $this->id_connection ) ) === FALSE )
        {
            return FALSE;
        }

        /* If PHP was compiled with PostgreSQL lib versions earlier
         * than 7.4, pg_version() won't return the server version
         * and so we'll have to fall back to running a query in
         * order to get it.
         */

        return isset( $pg_version[ 'server' ] )
            ? $this->data_cache[ 'version' ] = $pg_version[ 'server' ]
            : parent::version();
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
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        // Reset the transaction failure flag.
        // If the $test_mode flag is set to TRUE transactions will be rolled back
        // even if the queries produce a successful result.
        $this->_trans_failure = ( $test_mode === TRUE );

        return (bool)pg_query( $this->id_connection, 'BEGIN' );
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
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        return (bool)pg_query( $this->id_connection, 'COMMIT' );
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

        return (bool)pg_query( $this->id_connection, 'ROLLBACK' );
    }

    // --------------------------------------------------------------------

    /**
     * Determines if a query is a "write" type.
     *
     * @param   string $sql An SQL query string
     *
     * @access  public
     * @return  bool
     */
    public function is_write_type( $sql )
    {
        return (bool)preg_match( '/^\s*"?(SET|INSERT(?![^\)]+\)\s+RETURNING)|UPDATE(?!.*\sRETURNING)|DELETE|CREATE|DROP|TRUNCATE|LOAD|COPY|ALTER|RENAME|GRANT|REVOKE|LOCK|UNLOCK|REINDEX)\s/i', str_replace( array(
                                                                                                                                                                                                                 "\r\n",
                                                                                                                                                                                                                 "\r",
                                                                                                                                                                                                                 "\n"
                                                                                                                                                                                                             ), ' ', $sql ) );
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
        return pg_affected_rows( $this->id_result );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access  public
     * @return  string
     */
    public function insert_id()
    {
        $v = pg_version( $this->id_connection );
        $v = isset( $v[ 'server' ] ) ? $v[ 'server' ] : 0; // 'server' key is only available since PosgreSQL 7.4

        $table = ( func_num_args() > 0 ) ? func_get_arg( 0 ) : NULL;
        $column = ( func_num_args() > 1 ) ? func_get_arg( 1 ) : NULL;

        if( $table === NULL && $v >= '8.1' )
        {
            $sql = 'SELECT LASTVAL() AS ins_id';
        }
        elseif( $table !== NULL )
        {
            if( $column !== NULL && $v >= '8.0' )
            {
                $sql = 'SELECT pg_get_serial_sequence(\'' . $table . "', '" . $column . "') AS seq";
                $query = $this->query( $sql );
                $query = $query->row();
                $seq = $query->seq;
            }
            else
            {
                // seq_name passed in table parameter
                $seq = $table;
            }

            $sql = 'SELECT CURRVAL(\'' . $seq . "') AS ins_id";
        }
        else
        {
            return pg_last_oid( $this->id_result );
        }

        $query = $this->query( $sql );
        $query = $query->row();

        return (int)$query->ins_id;
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
        $sql = 'SELECT "column_name", "data_type", "character_maximum_length", "numeric_precision", "column_default"
			FROM "information_schema"."columns"
			WHERE LOWER("table_name") = ' . $this->escape( strtolower( $table ) );

        if( ( $query = $this->query( $sql ) ) === FALSE )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $data = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $query[ $i ]->column_name;
            $data[ $i ]->type = $query[ $i ]->data_type;
            $data[ $i ]->max_length = ( $query[ $i ]->character_maximum_length > 0 ) ? $query[ $i ]->character_maximum_length : $query[ $i ]->numeric_precision;
            $data[ $i ]->default = $query[ $i ]->column_default;
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
        return array( 'code' => '', 'message' => pg_last_error( $this->id_connection ) );
    }

    // --------------------------------------------------------------------

    /**
     * ORDER BY
     *
     * @param   string $order_by
     * @param   string $direction ASC, DESC or RANDOM
     * @param   bool   $escape
     *
     * @access  public
     * @return  object
     */
    public function order_by( $order_by, $direction = '', $escape = NULL )
    {
        $direction = strtoupper( trim( $direction ) );
        if( $direction === 'RANDOM' )
        {
            if( ! is_float( $order_by ) && ctype_digit( (string)$order_by ) )
            {
                $order_by = ( $order_by > 1 )
                    ? (float)'0.' . $order_by
                    : (float)$order_by;
            }

            if( is_float( $order_by ) )
            {
                $this->simple_query( 'SET SEED ' . $order_by );
            }

            $order_by = $this->_random_keywords[ 0 ];
            $direction = '';
            $escape = FALSE;
        }

        return parent::order_by( $order_by, $direction, $escape );
    }

    // --------------------------------------------------------------------

    /**
     * Set client character set
     *
     * @param   string $charset
     *
     * @access  protected
     * @return  bool
     */
    protected function _set_charset( $charset )
    {
        return ( pg_set_client_encoding( $this->id_connection, $charset ) === 0 );
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param   string $sql An SQL query
     *
     * @access  protected
     * @return  resource
     */
    protected function _execute( $sql )
    {
        return pg_query( $this->id_connection, $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @param   string $string
     *
     * @access  protected
     * @return  string
     */
    protected function _escape_string( $string )
    {
        return pg_escape_string( $this->id_connection, $string );
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
        $sql = 'SELECT "table_name" FROM "information_schema"."tables" WHERE "table_schema" = \'' . $this->schema . "'";

        if( $prefix_limit !== FALSE && $this->prefix_table !== '' )
        {
            return $sql . ' && "table_name" LIKE \''
                   . $this->escape_like_string( $this->prefix_table ) . "%' "
                   . sprintf( $this->_like_escape_string, $this->_like_escape_character );
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * List column query
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
        return 'SELECT "column_name"
			FROM "information_schema"."columns"
			WHERE LOWER("table_name") = ' . $this->escape( strtolower( $table ) );
    }

    // --------------------------------------------------------------------

    /**
     * "Smart" Escape String
     *
     * Escapes data based on type
     *
     * @param    string $string
     *
     * @access  public
     * @return  mixed
     */
    public function escape( $string )
    {
        if( is_php( '5.4.4' ) && ( is_string( $string ) OR ( is_object( $string ) && method_exists( $string, '__toString' ) ) ) )
        {
            return pg_escape_literal( $this->id_connection, $string );
        }
        elseif( is_bool( $string ) )
        {
            return ( $string ) ? 'TRUE' : 'FALSE';
        }

        return parent::escape( $string );
    }

    // --------------------------------------------------------------------

    /**
     * Update statement
     *
     * Generates a platform-specific update string from the supplied data
     *
     * @param   string $table
     * @param   array  $values
     *
     * @access  protected
     * @return  string
     */
    protected function _update( $table, $values )
    {
        $this->_limit = FALSE;
        $this->_order_by = array();

        return parent::_update( $table, $values );
    }

    // --------------------------------------------------------------------

    /**
     * Update_Batch statement
     *
     * Generates a platform-specific batch update string from the supplied data
     *
     * @param   string $table Table name
     * @param   array  $values Update data
     * @param   string $index WHERE key
     *
     * @access  protected
     * @return  string
     */
    protected function _update_batch( $table, $values, $index )
    {
        $ids = array();
        foreach( $values as $key => $val )
        {
            $ids[ ] = $val[ $index ];

            foreach( array_keys( $val ) as $field )
            {
                if( $field !== $index )
                {
                    $final[ $field ][ ] = 'WHEN ' . $val[ $index ] . ' THEN ' . $val[ $field ];
                }
            }
        }

        $cases = '';
        foreach( $final as $k => $v )
        {
            $cases .= $k . ' = (CASE ' . $index . "\n"
                      . implode( "\n", $v ) . "\n"
                      . 'ELSE ' . $k . ' END), ';
        }

        $this->where( $index . ' IN(' . implode( ',', $ids ) . ')', NULL, FALSE );

        return 'UPDATE ' . $table . ' SET ' . substr( $cases, 0, -2 ) . $this->_compile_where( '_where' );
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
        $this->_limit = FALSE;

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
        return $sql . ' LIMIT ' . $this->_limit . ( $this->_offset ? ' OFFSET ' . $this->_offset : '' );
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
        pg_close( $this->id_connection );
    }

}
