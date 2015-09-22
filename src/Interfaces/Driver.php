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

namespace O2System\O2DB\Interfaces;

// ------------------------------------------------------------------------

/**
 * Database Connector Driver Class
 *
 * Porting class from CodeIgniter Database Library
 *
 * This is the platform-independent base DB implementation class.
 * This class will not be called directly. Rather, the adapter
 * class for the specific database will extend and instantiate it.
 *
 * @package        O2System\Libraries\DB
 * @subpackage     Drivers
 * @category       Database
 * @author         Circle Creative Developer Team
 * @link
 */
abstract class Driver extends Query
{
    /**
     * Database driver
     *
     * @type    string
     */
    public $driver = NULL;


    /**
     * Database sub driver
     *
     * @type    string
     */
    public $sub_driver = NULL;

    /**
     * Data Source Name / Connect string
     *
     * @type    string
     */
    public $dsn;

    /**
     * Username
     *
     * @type    string
     */
    public $username;

    /**
     * Password
     *
     * @type    string
     */
    public $password;

    /**
     * Hostname
     *
     * @type    string
     */
    public $hostname;

    /**
     * Database port
     *
     * @type    int
     */
    public $port = '';

    /**
     * Database name
     *
     * @type    string
     */
    public $database;

    /**
     * Character set
     *
     * @type    string
     */
    public $charset = 'utf8';

    /**
     * Collation
     *
     * @type    string
     */
    public $collation = 'utf8_general_ci';

    /**
     * Encryption flag/data
     *
     * @type    mixed
     */
    public $encrypt = FALSE;

    /**
     * Table prefix
     *
     * @type    string
     */
    public $prefix_table = '';

    /**
     * Swap Prefix
     *
     * @type    string
     */
    public $prefix_swap = '';

    /**
     * Persistent connection flag
     *
     * @type    bool
     */
    public $persistent = FALSE;

    /**
     * Connection ID
     *
     * @type    object|resource
     */
    public $id_connection = FALSE;

    /**
     * Result ID
     *
     * @type    object|resource
     */
    public $id_result = FALSE;

    /**
     * Debug flag
     *
     * Whether to display error messages.
     *
     * @type    bool
     */
    public $debug_enabled = FALSE;

    /**
     * Benchmark time
     *
     * @type    int
     */
    public $benchmark = 0;

    /**
     * Executed queries count
     *
     * @type    int
     */
    public $query_count = 0;

    /**
     * Bind marker
     *
     * Character used to identify values in a prepared statement.
     *
     * @type    string
     */
    public $bind_marker = '?';

    /**
     * Save queries flag
     *
     * Whether to keep an in-memory history of queries for debugging purposes.
     *
     * @type    bool
     */
    public $save_queries = TRUE;

    /**
     * Queries list
     *
     * @see    O2System\Libraries\DB_driver::$save_queries
     * @type    string[]
     */
    public $queries = array();

    /**
     * Query times
     *
     * A list of times that queries took to execute.
     *
     * @type    array
     */
    public $query_times = array();

    /**
     * Data cache
     *
     * An internal generic value cache.
     *
     * @type    array
     */
    public $data_cache = array();

    /**
     * Transaction enabled flag
     *
     * @type    bool
     */
    public $trans_enabled = TRUE;

    /**
     * Strict transaction mode flag
     *
     * @type    bool
     */
    public $trans_strict = TRUE;

    /**
     * Transaction depth level
     *
     * @type    int
     */
    protected $_trans_depth = 0;

    /**
     * Transaction status flag
     *
     * Used with transactions to determine if a rollback should occur.
     *
     * @type    bool
     */
    protected $_trans_status = TRUE;

    /**
     * Transaction failure flag
     *
     * Used with transactions to determine if a transaction has failed.
     *
     * @type    bool
     */
    protected $_trans_failure = FALSE;

    /**
     * Protect identifiers flag
     *
     * @type    bool
     */
    protected $_protect_identifiers = TRUE;

    /**
     * List of reserved identifiers
     *
     * Identifiers that must NOT be escaped.
     *
     * @type    string[]
     */
    protected $_reserved_identifiers = array( '*' );

    /**
     * Identifier escape character
     *
     * @type    string
     */
    protected $_escape_character = '"';

    /**
     * ESCAPE statement string
     *
     * @type    string
     */
    protected $_like_escape_string = " ESCAPE '%s' ";

    /**
     * ESCAPE character
     *
     * @type    string
     */
    protected $_like_escape_character = '!';

    /**
     * ORDER BY random keywords
     *
     * @type    array
     */
    protected $_random_keywords = array( 'RAND()', 'RAND(%d)' );

    /**
     * COUNT string
     *
     * @used-by    O2System\Libraries\DB_driver::count_all()
     * @used-by    O2System\Libraries\DB_query_builder::count_all_results()
     *
     * @type    string
     */
    protected $_count_string = 'SELECT COUNT(*) AS ';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param    array $params
     */
    public function __construct( $params )
    {
        if( is_array( $params ) )
        {
            foreach( $params as $key => $val )
            {
                $this->$key = $val;
            }
        }

        $this->driver = str_replace( [ 'O2System\O2DB\\Drivers\\', '\\Driver' ], '', get_called_class() );
    }

    // --------------------------------------------------------------------

    /**
     * Initialize Database Settings
     *
     * @return    bool
     */
    public function initialize()
    {
        /* If an established connection is available, then there's
         * no need to connect and select the database.
         *
         * Depending on the database driver, conn_id can be either
         * boolean TRUE, a resource or an object.
         */
        if( $this->id_connection )
        {
            return TRUE;
        }

        // ----------------------------------------------------------------

        // Connect to the database and set the connection ID
        $this->id_connection = $this->connect( $this->persistent );

        // No connection resource? Check if there is a failover else throw an error
        if( ! $this->id_connection )
        {
            // Check if there is a failover set
            if( ! empty( $this->failover ) && is_array( $this->failover ) )
            {
                // Go over all the failovers
                foreach( $this->failover as $failover )
                {
                    // Replace the current settings with those of the failover
                    foreach( $failover as $key => $val )
                    {
                        $this->$key = $val;
                    }

                    // Try to connect
                    $this->id_connection = $this->connect( $this->persistent );

                    // If a connection is made break the foreach loop
                    if( $this->id_connection )
                    {
                        break;
                    }
                }
            }

            // We still don't have a connection?
            if( ! $this->id_connection )
            {
                Logger::error( 'Unable to connect to the database' );

                if( $this->debug_enabled )
                {
                    $this->display_error( 'db_unable_to_connect' );
                }

                return FALSE;
            }
        }

        // Now we set the character set and that's all
        return $this->set_charset( $this->charset );
    }

    // --------------------------------------------------------------------

    /**
     * DB connect
     *
     * This is just a dummy method that all drivers will override.
     *
     * @return      mixed
     */
    public function connect()
    {
        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Persistent database connection
     *
     * @return    mixed
     */
    public function pconnect()
    {
        return $this->connect( TRUE );
    }

    // --------------------------------------------------------------------

    /**
     * Reconnect
     *
     * Keep / reestablish the db connection if no queries have been
     * sent for a length of time exceeding the server's idle timeout.
     *
     * This is just a dummy method to allow drivers without such
     * functionality to not declare it, while others will override it.
     *
     * @return      void
     */
    public function reconnect()
    {
    }

    // --------------------------------------------------------------------

    /**
     * Set client character set
     *
     * @param    string
     *
     * @return bool
     * @throws \Exception
     */
    public function set_charset( $charset )
    {
        if( method_exists( $this, '_set_charset' ) && ! $this->_set_charset( $charset ) )
        {
            Logger::error( 'Unable to set database connection charset: ' . $charset );

            if( $this->debug_enabled )
            {
                throw new \Exception( 'Unable to set database connection charset: ' . $charset );
            }

            return FALSE;
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * The name of the platform in use (mysql, mssql, etc...)
     *
     * @return    string
     */
    public function platform()
    {
        return $this->driver;
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * Returns a string containing the version of the database being used.
     * Most drivers will override this method.
     *
     * @return string
     * @throws \Exception
     */
    public function version()
    {
        if( isset( $this->data_cache[ 'version' ] ) )
        {
            return $this->data_cache[ 'version' ];
        }

        if( FALSE === ( $sql = $this->_version() ) )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'This feature is not available for the database you are using.' );
            }

            return FALSE;
        }

        $query = $this->query( $sql )->row();

        return $this->data_cache[ 'version' ] = $query->ver;
    }

    // --------------------------------------------------------------------

    /**
     * Version number query string
     *
     * @return    string
     */
    protected function _version()
    {
        return 'SELECT VERSION() AS ver';
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * Accepts an SQL string as input and returns a result object upon
     * successful execution of a "read" type query. Returns boolean TRUE
     * upon successful execution of a "write" type query. Returns boolean
     * FALSE upon failure, and if the $debug_mode variable is set to TRUE
     * will raise an error.
     *
     * @param    string  $sql
     * @param array|bool $binds         = FALSE        An array of binding data
     * @param    bool    $return_object = NULL
     *
     * @return mixed
     * @throws \Exception
     */
    public function query( $sql, $binds = FALSE, $return_object = NULL )
    {
        if( $sql === '' )
        {
            Logger::error( 'Invalid query: ' . $sql );

            if( $this->debug_enabled )
            {
                throw new \Exception( 'The query you submitted is not valid.' );
            }

            return FALSE;
        }
        elseif( ! is_bool( $return_object ) )
        {
            $return_object = ! $this->is_write_type( $sql );
        }

        // Verify table prefix and replace if necessary
        if( $this->prefix_table !== '' && $this->prefix_swap !== '' && $this->prefix_table !== $this->prefix_swap )
        {
            $sql = preg_replace( '/(\W)' . $this->prefix_swap . '(\S+?)/', '\\1' . $this->prefix_table . '\\2', $sql );
        }

        // Compile binds if needed
        if( $binds !== FALSE )
        {
            $sql = $this->compile_binds( $sql, $binds );
        }

        // Save the query for debugging
        if( $this->save_queries === TRUE )
        {
            $this->queries[ ] = $sql;
        }

        // Start the Query Timer
        $time_start = microtime( TRUE );

        // Run the Query
        if( FALSE === ( $this->id_result = $this->simple_query( $sql ) ) )
        {
            if( $this->save_queries === TRUE )
            {
                $this->query_times[ ] = 0;
            }

            // This will trigger a rollback if transactions are being used
            $this->_trans_status = FALSE;

            // Grab the error now, as we might run some additional queries before displaying the error
            $error = $this->error();

            // Log errors
            Logger::error( 'Query error: ' . $error[ 'message' ] . ' - Invalid query: ' . $sql );

            if( $this->debug_enabled )
            {
                // We call this function in order to roll-back queries
                // if transactions are enabled. If we don't call this here
                // the error message will trigger an exit, causing the
                // transactions to remain in limbo.
                if( $this->_trans_depth !== 0 )
                {
                    do
                    {
                        $this->trans_complete();
                    }
                    while( $this->_trans_depth !== 0 );
                }

                // Display errors
                throw new \Exception( $error[ 'code' ] . ':' . $error[ 'message' ] . '<br><br>' . $sql );
            }

            return FALSE;
        }

        // Stop and aggregate the query time results
        $time_end = microtime( TRUE );
        $this->benchmark += $time_end - $time_start;

        if( $this->save_queries === TRUE )
        {
            $this->query_times[ ] = $time_end - $time_start;
        }

        // Increment the query counter
        $this->query_count++;

        // Will we have a result object instantiated? If not - we'll simply return TRUE
        if( $return_object !== TRUE )
        {
            return TRUE;
        }

        // Load and instantiate the result driver
        $result_class_name = 'O2System\\O2DB\\Drivers\\' . ucfirst( $this->driver_name ) . '\\Result';

        return new $result_class_name( $this );
    }

    // --------------------------------------------------------------------

    /**
     * Simple Query
     * This is a simplified version of the query() function. Internally
     * we only use it when running transaction commands since they do
     * not require all the features of the main query() function.
     *
     * @param    string    the sql query
     *
     * @return    mixed
     */
    public function simple_query( $sql )
    {
        if( ! $this->id_connection )
        {
            $this->initialize();
        }

        return $this->_execute( $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Disable Transactions
     * This permits transactions to be disabled at run-time.
     *
     * @return    void
     */
    public function trans_off()
    {
        $this->trans_enabled = FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Enable/disable Transaction Strict Mode
     * When strict mode is enabled, if you are running multiple groups of
     * transactions, if one group fails all groups will be rolled back.
     * If strict mode is disabled, each group is treated autonomously, meaning
     * a failure of one group will not affect any others
     *
     * @param    bool $mode = TRUE
     *
     * @return    void
     */
    public function trans_strict( $mode = TRUE )
    {
        $this->trans_strict = is_bool( $mode ) ? $mode : TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Start Transaction
     *
     * @param    bool $test_mode = FALSE
     *
     * @return    void
     */
    public function trans_start( $test_mode = FALSE )
    {
        if( ! $this->trans_enabled )
        {
            return;
        }

        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( $this->_trans_depth > 0 )
        {
            $this->_trans_depth += 1;

            return;
        }

        $this->trans_begin( $test_mode );
        $this->_trans_depth += 1;
    }

    // --------------------------------------------------------------------

    /**
     * Complete Transaction
     *
     * @return    bool
     */
    public function trans_complete()
    {
        if( ! $this->trans_enabled )
        {
            return FALSE;
        }

        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( $this->_trans_depth > 1 )
        {
            $this->_trans_depth -= 1;

            return TRUE;
        }
        else
        {
            $this->_trans_depth = 0;
        }

        // The query() function will set this flag to FALSE in the event that a query failed
        if( $this->_trans_status === FALSE OR $this->_trans_failure === TRUE )
        {
            $this->trans_rollback();

            // If we are NOT running in strict mode, we will reset
            // the _trans_status flag so that subsequent groups of transactions
            // will be permitted.
            if( $this->trans_strict === FALSE )
            {
                $this->_trans_status = TRUE;
            }

            Logger::debug( 'DB Transaction Failure' );

            return FALSE;
        }

        $this->trans_commit();

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * Lets you retrieve the transaction flag to determine if it has failed
     *
     * @return    bool
     */
    public function trans_status()
    {
        return $this->_trans_status;
    }

    // --------------------------------------------------------------------

    /**
     * Compile Bindings
     *
     * @param    string    the sql statement
     * @param    array     an array of bind data
     *
     * @return    string
     */
    public function compile_binds( $sql, $binds )
    {
        if( empty( $binds ) OR empty( $this->bind_marker ) OR strpos( $sql, $this->bind_marker ) === FALSE )
        {
            return $sql;
        }
        elseif( ! is_array( $binds ) )
        {
            $binds = array( $binds );
            $bind_count = 1;
        }
        else
        {
            // Make sure we're using numeric keys
            $binds = array_values( $binds );
            $bind_count = count( $binds );
        }

        // We'll need the marker length later
        $ml = strlen( $this->bind_marker );

        // Make sure not to replace a chunk inside a string that happens to match the bind marker
        if( $c = preg_match_all( "/'[^']*'/i", $sql, $matches ) )
        {
            $c = preg_match_all( '/' . preg_quote( $this->bind_marker, '/' ) . '/i',
                                 str_replace( $matches[ 0 ],
                                              str_replace( $this->bind_marker, str_repeat( ' ', $ml ), $matches[ 0 ] ),
                                              $sql, $c ),
                                 $matches, PREG_OFFSET_CAPTURE );

            // Bind values' count must match the count of markers in the query
            if( $bind_count !== $c )
            {
                return $sql;
            }
        }
        elseif( ( $c = preg_match_all( '/' . preg_quote( $this->bind_marker, '/' ) . '/i', $sql, $matches,
                                       PREG_OFFSET_CAPTURE ) ) !== $bind_count
        )
        {
            return $sql;
        }

        do
        {
            $c--;
            $escaped_value = $this->escape( $binds[ $c ] );
            if( is_array( $escaped_value ) )
            {
                $escaped_value = '(' . implode( ',', $escaped_value ) . ')';
            }
            $sql = substr_replace( $sql, $escaped_value, $matches[ 0 ][ $c ][ 1 ], $ml );
        }
        while( $c !== 0 );

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Determines if a query is a "write" type.
     *
     * @param    string    An SQL query string
     *
     * @return    bool
     */
    public function is_write_type( $sql )
    {
        return (bool)preg_match( '/^\s*"?(SET|INSERT|UPDATE|DELETE|REPLACE|CREATE|DROP|TRUNCATE|LOAD|COPY|ALTER|RENAME|GRANT|REVOKE|LOCK|UNLOCK|REINDEX)\s/i',
                                 $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Calculate the aggregate query elapsed time
     *
     * @param    int    The number of decimal places
     *
     * @return    string
     */
    public function elapsed_time( $decimals = 6 )
    {
        return number_format( $this->benchmark, $decimals );
    }

    // --------------------------------------------------------------------

    /**
     * Returns the total number of queries
     *
     * @return    int
     */
    public function total_queries()
    {
        return $this->query_count;
    }

    // --------------------------------------------------------------------

    /**
     * Returns the last query that was executed
     *
     * @return    string
     */
    public function last_query()
    {
        return end( $this->queries );
    }

    // --------------------------------------------------------------------

    /**
     * "Smart" Escape String
     *
     * Escapes data based on type
     * Sets boolean and null types
     *
     * @param    string
     *
     * @return    mixed
     */
    public function escape( $string )
    {
        if( is_array( $string ) )
        {
            $string = array_map( array( &$this, 'escape' ), $string );

            return $string;
        }
        elseif( is_string( $string ) OR ( is_object( $string ) && method_exists( $string, '__toString' ) ) )
        {
            return "'" . $this->escape_string( $string ) . "'";
        }
        elseif( is_bool( $string ) )
        {
            return ( $string === FALSE ) ? 0 : 1;
        }
        elseif( $string === NULL )
        {
            return 'NULL';
        }

        return $string;
    }

    // --------------------------------------------------------------------

    /**
     * Escape String
     *
     * @param    string|string[] $string Input string
     * @param    bool            $like   Whether or not the string will be used in a LIKE condition
     *
     * @return    string
     */
    public function escape_string( $string, $like = FALSE )
    {
        if( is_array( $string ) )
        {
            foreach( $string as $key => $val )
            {
                $string[ $key ] = $this->escape_string( $val, $like );
            }

            return $string;
        }

        $string = $this->_escape_string( $string );

        // escape LIKE condition wildcards
        if( $like === TRUE )
        {
            return str_replace(
                array( $this->_like_escape_character, '%', '_' ),
                array(
                    $this->_like_escape_character . $this->_like_escape_character, $this->_like_escape_character . '%',
                    $this->_like_escape_character . '_'
                ),
                $string
            );
        }

        return $string;
    }

    // --------------------------------------------------------------------

    /**
     * Escape LIKE String
     *
     * Calls the individual driver for platform
     * specific escaping for LIKE conditions
     *
     * @param    string|string[]
     *
     * @return    mixed
     */
    public function escape_like_string( $str )
    {
        return $this->escape_string( $str, TRUE );
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @param    string
     *
     * @return    string
     */
    protected function _escape_string( $string )
    {
        $non_displayables[ ] = '/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]+/S';    // 00-08, 11, 12, 14-31, 127

        do
        {
            $string = preg_replace( $non_displayables, '', $string, -1, $count );
        }
        while( $count );

        return str_replace( "'", "''", $string );
    }

    // --------------------------------------------------------------------

    /**
     * Primary
     *
     * Retrieves the primary key. It assumes that the row in the first
     * position is the primary key
     *
     * @param    string $table Table name
     *
     * @return    string
     */
    public function primary( $table )
    {
        $fields = $this->list_fields( $table );

        return is_array( $fields ) ? current( $fields ) : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * "Count All" query
     *
     * Generates a platform-specific query string that counts all records in
     * the specified database
     *
     * @param    string
     *
     * @return    int
     */
    public function count_all( $table = '' )
    {
        if( $table === '' )
        {
            return 0;
        }

        $query = $this->query( $this->_count_string . $this->escape_identifiers( 'numrows' ) . ' FROM ' . $this->protect_identifiers( $table, TRUE, FALSE ) );
        if( $query->num_rows() === 0 )
        {
            return 0;
        }

        $query = $query->row();
        $this->_reset_select();

        return (int)$query->numrows;
    }

    // --------------------------------------------------------------------

    /**
     * Returns an array of table names
     *
     * @param bool|string $constrain_by_prefix = FALSE
     *
     * @return array
     * @throws \Exception
     */
    public function list_tables( $constrain_by_prefix = FALSE )
    {
        // Is there a cached result?
        if( isset( $this->data_cache[ 'table_names' ] ) )
        {
            return $this->data_cache[ 'table_names' ];
        }

        if( FALSE === ( $sql = $this->_list_tables( $constrain_by_prefix ) ) )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'This feature is not available for the database you are using.' );
            }

            return FALSE;
        }

        $this->data_cache[ 'table_names' ] = array();
        $query = $this->query( $sql );

        foreach( $query->result_array() as $row )
        {
            // Do we know from which column to get the table name?
            if( ! isset( $key ) )
            {
                if( isset( $row[ 'table_name' ] ) )
                {
                    $key = 'table_name';
                }
                elseif( isset( $row[ 'TABLE_NAME' ] ) )
                {
                    $key = 'TABLE_NAME';
                }
                else
                {
                    /* We have no other choice but to just get the first element's key.
                     * Due to array_shift() accepting its argument by reference, if
                     * E_STRICT is on, this would trigger a warning. So we'll have to
                     * assign it first.
                     */
                    $key = array_keys( $row );
                    $key = array_shift( $key );
                }
            }

            $this->data_cache[ 'table_names' ][ ] = $row[ $key ];
        }

        return $this->data_cache[ 'table_names' ];
    }

    // --------------------------------------------------------------------

    /**
     * Determine if a particular table exists
     *
     * @param    string $table_name
     *
     * @return    bool
     */
    public function table_exists( $table_name )
    {
        return in_array( $this->protect_identifiers( $table_name, TRUE, FALSE, FALSE ), $this->list_tables() );
    }

    // --------------------------------------------------------------------

    /**
     * Fetch Field Names
     *
     * @param    string    the table name
     *
     * @return array
     * @throws \Exception
     */
    public function list_fields( $table )
    {
        // Is there a cached result?
        if( isset( $this->data_cache[ 'field_names' ][ $table ] ) )
        {
            return $this->data_cache[ 'field_names' ][ $table ];
        }

        if( FALSE === ( $sql = $this->_list_columns( $table ) ) )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'This feature is not available for the database you are using.' );
            }

            return FALSE;
        }

        $query = $this->query( $sql );
        $this->data_cache[ 'field_names' ][ $table ] = array();

        foreach( $query->result_array() as $row )
        {
            // Do we know from where to get the column's name?
            if( ! isset( $key ) )
            {
                if( isset( $row[ 'column_name' ] ) )
                {
                    $key = 'column_name';
                }
                elseif( isset( $row[ 'COLUMN_NAME' ] ) )
                {
                    $key = 'COLUMN_NAME';
                }
                else
                {
                    // We have no other choice but to just get the first element's key.
                    $key = key( $row );
                }
            }

            $this->data_cache[ 'field_names' ][ $table ][ ] = $row[ $key ];
        }

        return $this->data_cache[ 'field_names' ][ $table ];
    }

    // --------------------------------------------------------------------

    /**
     * Determine if a particular field exists
     *
     * @param    string
     * @param    string
     *
     * @return    bool
     */
    public function field_exists( $field_name, $table_name )
    {
        return in_array( $field_name, $this->list_fields( $table_name ) );
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param    string $table the table name
     *
     * @return    array
     */
    public function field_data( $table )
    {
        $query = $this->query( $this->_field_data( $this->protect_identifiers( $table, TRUE, NULL, FALSE ) ) );

        return ( $query ) ? $query->field_data() : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Escape the SQL Identifiers
     *
     * This function escapes column and table names
     *
     * @param    mixed
     *
     * @return    mixed
     */
    public function escape_identifiers( $item )
    {
        if( $this->_escape_character === '' OR
            empty( $item ) OR
            in_array( $item, $this->_reserved_identifiers )
        )
        {
            return $item;
        }
        elseif( is_array( $item ) )
        {
            foreach( $item as $key => $value )
            {
                $item[ $key ] = $this->escape_identifiers( $value );
            }

            return $item;
        }
        // Avoid breaking functions and literal values inside queries
        elseif( ctype_digit( $item ) OR
                $item[ 0 ] === "'" OR
                ( $this->_escape_character !== '"' && $item[ 0 ] === '"' ) OR
                strpos( $item, '(' ) !== FALSE
        )
        {
            return $item;
        }

        static $preg_ec = array();

        if( empty( $preg_ec ) )
        {
            if( is_array( $this->_escape_character ) )
            {
                $preg_ec = array(
                    preg_quote( $this->_escape_character[ 0 ], '/' ),
                    preg_quote( $this->_escape_character[ 1 ], '/' ),
                    $this->_escape_character[ 0 ],
                    $this->_escape_character[ 1 ]
                );
            }
            else
            {
                $preg_ec[ 0 ] = $preg_ec[ 1 ] = preg_quote( $this->_escape_character, '/' );
                $preg_ec[ 2 ] = $preg_ec[ 3 ] = $this->_escape_character;
            }
        }

        foreach( $this->_reserved_identifiers as $id )
        {
            if( strpos( $item, '.' . $id ) !== FALSE )
            {
                return preg_replace( '/' . $preg_ec[ 0 ] . '?([^' . $preg_ec[ 1 ] . '\.]+)' . $preg_ec[ 1 ] . '?\./i',
                                     $preg_ec[ 2 ] . '$1' . $preg_ec[ 3 ] . '.', $item );
            }
        }

        return preg_replace( '/' . $preg_ec[ 0 ] . '?([^' . $preg_ec[ 1 ] . '\.]+)' . $preg_ec[ 1 ] . '?(\.)?/i',
                             $preg_ec[ 2 ] . '$1' . $preg_ec[ 3 ] . '$2', $item );
    }

    // --------------------------------------------------------------------

    /**
     * Generate an insert string
     *
     * @param    string    the table upon which the query will be performed
     * @param    array     an associative array data of key/values
     *
     * @return    string
     */
    public function insert_string( $table, $data )
    {
        $fields = $values = array();

        foreach( $data as $key => $val )
        {
            $fields[ ] = $this->escape_identifiers( $key );
            $values[ ] = $this->escape( $val );
        }

        return $this->_insert( $this->protect_identifiers( $table, TRUE, NULL, FALSE ), $fields, $values );
    }

    // --------------------------------------------------------------------

    /**
     * Insert statement
     *
     * Generates a platform-specific insert string from the supplied data
     *
     * @param    string    the table name
     * @param    array     the insert keys
     * @param    array     the insert values
     *
     * @return    string
     */
    protected function _insert( $table, $keys, $values )
    {
        return 'INSERT INTO ' . $table . ' (' . implode( ', ', $keys ) . ') VALUES (' . implode( ', ', $values ) . ')';
    }

    // --------------------------------------------------------------------

    /**
     * Generate an update string
     *
     * @param    string    the table upon which the query will be performed
     * @param    array     an associative array data of key/values
     * @param    mixed     the "where" statement
     *
     * @return    string
     */
    public function update_string( $table, $data, $where )
    {
        if( empty( $where ) )
        {
            return FALSE;
        }

        $this->where( $where );

        $fields = array();
        foreach( $data as $key => $val )
        {
            $fields[ $this->protect_identifiers( $key ) ] = $this->escape( $val );
        }

        $sql = $this->_update( $this->protect_identifiers( $table, TRUE, NULL, FALSE ), $fields );
        $this->_reset_write();

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Update statement
     *
     * Generates a platform-specific update string from the supplied data
     *
     * @param    string    the table name
     * @param    array     the update data
     *
     * @return    string
     */
    protected function _update( $table, $values )
    {
        foreach( $values as $key => $val )
        {
            $valstr[ ] = $key . ' = ' . $val;
        }

        return 'UPDATE ' . $table . ' SET ' . implode( ', ', $valstr )
               . $this->_compile_where( '_where' )
               . $this->_compile_order_by()
               . ( $this->_limit ? ' LIMIT ' . $this->_limit : '' );
    }

    // --------------------------------------------------------------------

    /**
     * Tests whether the string has an SQL operator
     *
     * @param    string
     *
     * @return    bool
     */
    protected function _has_operator( $str )
    {
        return (bool)preg_match( '/(<|>|!|=|\sIS NULL|\sIS NOT NULL|\sEXISTS|\sBETWEEN|\sLIKE|\sIN\s*\(|\s)/i',
                                 trim( $str ) );
    }

    // --------------------------------------------------------------------

    /**
     * Returns the SQL string operator
     *
     * @param    string
     *
     * @return    string
     */
    protected function _get_operator( $str )
    {
        static $_operators;

        if( empty( $_operators ) )
        {
            $_les = ( $this->_like_escape_string !== '' )
                ? '\s+' . preg_quote( trim( sprintf( $this->_like_escape_string, $this->_like_escape_character ) ), '/' )
                : '';
            $_operators = array(
                '\s*(?:<|>|!)?=\s*',        // =, <=, >=, !=
                '\s*<>?\s*',            // <, <>
                '\s*>\s*',            // >
                '\s+IS NULL',            // IS NULL
                '\s+IS NOT NULL',        // IS NOT NULL
                '\s+EXISTS\s*\([^\)]+\)',    // EXISTS(sql)
                '\s+NOT EXISTS\s*\([^\)]+\)',    // NOT EXISTS(sql)
                '\s+BETWEEN\s+\S+\s+AND\s+\S+',    // BETWEEN value AND value
                '\s+IN\s*\([^\)]+\)',        // IN(list)
                '\s+NOT IN\s*\([^\)]+\)',    // NOT IN (list)
                '\s+LIKE\s+\S+' . $_les,        // LIKE 'expr'[ ESCAPE '%s']
                '\s+NOT LIKE\s+\S+' . $_les    // NOT LIKE 'expr'[ ESCAPE '%s']
            );

        }

        return preg_match( '/' . implode( '|', $_operators ) . '/i', $str, $match )
            ? $match[ 0 ] : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Enables a native PHP function to be run, using a platform agnostic wrapper.
     *
     * @param    string $function Function name
     *
     * @return mixed
     * @throws \Exception
     */
    public function call_function( $function )
    {
        $driver = ( $this->driver === 'postgre' ) ? 'pg_' : $this->driver . '_';

        if( FALSE === strpos( $driver, $function ) )
        {
            $function = $driver . $function;
        }

        if( ! function_exists( $function ) )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'This feature is not available for the database you are using.' );
            }

            return FALSE;
        }

        return ( func_num_args() > 1 )
            ? call_user_func_array( $function, array_slice( func_get_args(), 1 ) )
            : call_user_func( $function );
    }

    // --------------------------------------------------------------------

    /**
     * Close DB Connection
     *
     * @return    void
     */
    public function close()
    {
        if( $this->id_connection )
        {
            $this->_close();
            $this->id_connection = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Close DB Connection
     *
     * This method would be overridden by most of the drivers.
     *
     * @return    void
     */
    protected function _close()
    {
        $this->id_connection = FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Protect Identifiers
     *
     * This function is used extensively by the Query Builder class, and by
     * a couple functions in this class.
     * It takes a column or table name (optionally with an alias) and inserts
     * the table prefix onto it. Some logic is necessary in order to deal with
     * column names that include the path. Consider a query like this:
     *
     * SELECT * FROM hostname.database.table.column AS c FROM hostname.database.table
     *
     * Or a query with aliasing:
     *
     * SELECT m.member_id, m.member_name FROM members AS m
     *
     * Since the column name can include up to four segments (host, DB, table, column)
     * or also have an alias prefix, we need to do a bit of work to figure this out and
     * insert the table prefix (if it exists) in the proper position, and escape only
     * the correct identifiers.
     *
     * @param    string
     * @param    bool
     * @param    mixed
     * @param    bool
     *
     * @return    string
     */
    public function protect_identifiers( $item, $prefix_single = FALSE, $protect_identifiers = NULL, $field_exists = TRUE )
    {
        if( ! is_bool( $protect_identifiers ) )
        {
            $protect_identifiers = $this->_protect_identifiers;
        }

        if( is_array( $item ) )
        {
            $escaped_array = array();
            foreach( $item as $k => $v )
            {
                $escaped_array[ $this->protect_identifiers( $k ) ] = $this->protect_identifiers( $v, $prefix_single,
                                                                                                 $protect_identifiers,
                                                                                                 $field_exists );
            }

            return $escaped_array;
        }

        // This is basically a bug fix for queries that use MAX, MIN, etc.
        // If a parenthesis is found we know that we do not need to
        // escape the data or add a prefix. There's probably a more graceful
        // way to deal with this, but I'm not thinking of it -- Rick
        //
        // Added exception for single quotes as well, we don't want to alter
        // literal strings. -- Narf
        if( strpos( $item, '(' ) !== FALSE OR strpos( $item, "'" ) !== FALSE )
        {
            return $item;
        }

        // Convert tabs or multiple spaces into single spaces
        $item = preg_replace( '/\s+/', ' ', $item );

        // If the item has an alias declaration we remove it and set it aside.
        // Note: strripos() is used in order to support spaces in table names
        if( $offset = strripos( $item, ' AS ' ) )
        {
            $alias = ( $protect_identifiers )
                ? substr( $item, $offset, 4 ) . $this->escape_identifiers( substr( $item, $offset + 4 ) )
                : substr( $item, $offset );
            $item = substr( $item, 0, $offset );
        }
        elseif( $offset = strrpos( $item, ' ' ) )
        {
            $alias = ( $protect_identifiers )
                ? ' ' . $this->escape_identifiers( substr( $item, $offset + 1 ) )
                : substr( $item, $offset );
            $item = substr( $item, 0, $offset );
        }
        else
        {
            $alias = '';
        }

        // Break the string apart if it contains periods, then insert the table prefix
        // in the correct location, assuming the period doesn't indicate that we're dealing
        // with an alias. While we're at it, we will escape the components
        if( strpos( $item, '.' ) !== FALSE )
        {
            $parts = explode( '.', $item );

            // Does the first segment of the exploded item match
            // one of the aliases previously identified? If so,
            // we have nothing more to do other than escape the item
            if( in_array( $parts[ 0 ], $this->_aliased_tables ) )
            {
                if( $protect_identifiers === TRUE )
                {
                    foreach( $parts as $key => $val )
                    {
                        if( ! in_array( $val, $this->_reserved_identifiers ) )
                        {
                            $parts[ $key ] = $this->escape_identifiers( $val );
                        }
                    }

                    $item = implode( '.', $parts );
                }

                return $item . $alias;
            }

            // Is there a table prefix defined in the config file? If not, no need to do anything
            if( $this->prefix_table !== '' )
            {
                // We now add the table prefix based on some logic.
                // Do we have 4 segments (hostname.database.table.column)?
                // If so, we add the table prefix to the column name in the 3rd segment.
                if( isset( $parts[ 3 ] ) )
                {
                    $i = 2;
                }
                // Do we have 3 segments (database.table.column)?
                // If so, we add the table prefix to the column name in 2nd position
                elseif( isset( $parts[ 2 ] ) )
                {
                    $i = 1;
                }
                // Do we have 2 segments (table.column)?
                // If so, we add the table prefix to the column name in 1st segment
                else
                {
                    $i = 0;
                }

                // This flag is set when the supplied $item does not contain a field name.
                // This can happen when this function is being called from a JOIN.
                if( $field_exists === FALSE )
                {
                    $i++;
                }

                // Verify table prefix and replace if necessary
                if( $this->prefix_swap !== '' && strpos( $parts[ $i ], $this->prefix_swap ) === 0 )
                {
                    $parts[ $i ] = preg_replace( '/^' . $this->prefix_swap . '(\S+?)/', $this->prefix_table . '\\1',
                                                 $parts[ $i ] );
                }
                // We only add the table prefix if it does not already exist
                elseif( strpos( $parts[ $i ], $this->prefix_table ) !== 0 )
                {
                    $parts[ $i ] = $this->prefix_table . $parts[ $i ];
                }

                // Put the parts back together
                $item = implode( '.', $parts );
            }

            if( $protect_identifiers === TRUE )
            {
                $item = $this->escape_identifiers( $item );
            }

            return $item . $alias;
        }

        // Is there a table prefix? If not, no need to insert it
        if( $this->prefix_table !== '' )
        {
            // Verify table prefix and replace if necessary
            if( $this->prefix_swap !== '' && strpos( $item, $this->prefix_swap ) === 0 )
            {
                $item = preg_replace( '/^' . $this->prefix_swap . '(\S+?)/', $this->prefix_table . '\\1', $item );
            }
            // Do we prefix an item with no segments?
            elseif( $prefix_single === TRUE && strpos( $item, $this->prefix_table ) !== 0 )
            {
                $item = $this->prefix_table . $item;
            }
        }

        if( $protect_identifiers === TRUE && ! in_array( $item, $this->_reserved_identifiers ) )
        {
            $item = $this->escape_identifiers( $item );
        }

        return $item . $alias;
    }

    // --------------------------------------------------------------------

    /**
     * Dummy method that allows Query Builder class to be disabled
     * and keep count_all() working.
     *
     * @return    void
     */
    protected function _reset_select()
    {
    }

}