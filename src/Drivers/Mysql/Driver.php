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

namespace O2System\O2DB\Drivers\Mysql;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * MySQL Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * Compression flag
     *
     * @access  public
     * @type    bool
     */
    public $compress = FALSE;

    /**
     * DELETE hack flag
     *
     * Whether to use the MySQL "delete hack" which allows the number
     * of affected rows to be shown. Uses a preg_replace when enabled,
     * adding a bit more processing to all queries.
     *
     * @access  public
     * @type    bool
     */
    public $delete_hack = TRUE;

    /**
     * Strict ON flag
     *
     * Whether we're running in strict SQL mode.
     *
     * @access  public
     * @type    bool
     */
    public $stricton = FALSE;

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * @access  public
     * @type    string
     */
    protected $_escape_character = '`';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param   array $params
     *
     * @access  public
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( ! empty( $this->port ) )
        {
            $this->hostname .= ':' . $this->port;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Non-persistent database connection
     *
     * @param   bool $persistent
     *
     * @access  public
     * @return  resource
     * @throws  \Exception
     */
    public function connect( $persistent = FALSE )
    {
        $client_flags = ( $this->compress === FALSE ) ? 0 : MYSQL_CLIENT_COMPRESS;

        if( $this->encrypt === TRUE )
        {
            $client_flags = $client_flags | MYSQL_CLIENT_SSL;
        }

        // Error suppression is necessary mostly due to PHP 5.5+ issuing E_DEPRECATED messages
        $this->id_connection = ( $persistent === TRUE )
            ? mysql_pconnect( $this->hostname, $this->username, $this->password, $client_flags )
            : mysql_connect( $this->hostname, $this->username, $this->password, TRUE, $client_flags );

        // ----------------------------------------------------------------

        // Select the DB... assuming a database name is specified in the config file
        if( $this->database !== '' && ! $this->select() )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'Unable to select the specified database: ' . $this->database );
            }

            return FALSE;
        }

        if( $this->stricton && is_resource( $this->id_connection ) )
        {
            $this->simple_query( 'SET SESSION sql_mode="STRICT_ALL_TABLES"' );
        }

        return $this->id_connection;
    }

    // --------------------------------------------------------------------

    /**
     * Select the database
     *
     * @param   string $database
     *
     * @access  public
     * @return  bool
     */
    public function select( $database = '' )
    {
        if( $database === '' )
        {
            $database = $this->database;
        }

        if( mysql_select_db( $database, $this->id_connection ) )
        {
            $this->database = $database;

            return TRUE;
        }

        return FALSE;
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
        if( mysql_ping( $this->id_connection ) === FALSE )
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

        if( ! $this->id_connection OR ( $version = mysql_get_server_info( $this->id_connection ) ) === FALSE )
        {
            return FALSE;
        }

        return $this->data_cache[ 'version' ] = $version;
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

        $this->simple_query( 'SET AUTOCOMMIT=0' );
        $this->simple_query( 'START TRANSACTION' ); // can also be BEGIN or BEGIN WORK
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
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        $this->simple_query( 'COMMIT' );
        $this->simple_query( 'SET AUTOCOMMIT=1' );

        return TRUE;
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

        $this->simple_query( 'ROLLBACK' );
        $this->simple_query( 'SET AUTOCOMMIT=1' );

        return TRUE;
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
        return mysql_affected_rows( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access  public
     * @return  int
     */
    public function insert_id()
    {
        return mysql_insert_id( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param   string $table
     *
     * @access  protected
     * @return  array
     */
    public function field_data( $table )
    {
        if( ( $query = $this->query( 'SHOW COLUMNS FROM ' . $this->protect_identifiers( $table, TRUE, NULL, FALSE ) ) ) === FALSE )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $result = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $result[ $i ] = new \stdClass();
            $result[ $i ]->name = $query[ $i ]->Field;

            sscanf( $query[ $i ]->Type, '%[a-z](%d)',
                    $result[ $i ]->type,
                    $result[ $i ]->max_length
            );

            $result[ $i ]->default = $query[ $i ]->Default;
            $result[ $i ]->primary_key = (int)( $query[ $i ]->Key === 'PRI' );
        }

        return $result;
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occured.
     *
     * @access  protected
     * @return  array
     */
    public function error()
    {
        return array( 'code' => mysql_errno( $this->id_connection ), 'message' => mysql_error( $this->id_connection ) );
    }

    // --------------------------------------------------------------------

    /**
     * Set client character set
     *
     * @param   string $charset
     *
     * @access  public
     * @return  bool
     */
    protected function _set_charset( $charset )
    {
        return mysql_set_charset( $charset, $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param   string $sql An SQL query
     *
     * @access  public
     * @return  mixed
     */
    protected function _execute( $sql )
    {
        return mysql_query( $this->_prep_query( $sql ), $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Prep the query
     *
     * If needed, each database adapter can prep the query string
     *
     * @param   string $sql An SQL query
     *
     * @access  protected
     * @return  string
     */
    protected function _prep_query( $sql )
    {
        // mysql_affected_rows() returns 0 for "DELETE FROM TABLE" queries. This hack
        // modifies the query so that it a proper number of affected rows is returned.
        if( $this->delete_hack === TRUE && preg_match( '/^\s*DELETE\s+FROM\s+(\S+)\s*$/i', $sql ) )
        {
            return trim( $sql ) . ' WHERE 1=1';
        }

        return $sql;
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
        return mysql_real_escape_string( $string, $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * List table query
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
        $sql = 'SHOW TABLES FROM ' . $this->escape_identifiers( $this->database );

        if( $prefix_limit !== FALSE && $this->prefix_table !== '' )
        {
            return $sql . " LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%'";
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
        return 'SHOW COLUMNS FROM ' . $this->protect_identifiers( $table, TRUE, NULL, FALSE );
    }

    // --------------------------------------------------------------------

    /**
     * FROM tables
     *
     * Groups tables in FROM clauses if needed, so there is no confusion
     * about operator precedence.
     *
     * @access  protected
     * @return  string
     */
    protected function _from_tables()
    {
        if( ! empty( $this->_join ) && count( $this->_from ) > 1 )
        {
            return '(' . implode( ', ', $this->_from ) . ')';
        }

        return implode( ', ', $this->_from );
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
        // Error suppression to avoid annoying E_WARNINGs in cases
        // where the connection has already been closed for some reason.
        @mysql_close( $this->id_connection );
    }

}
