<?php
/**
 * O2System
 *
 * An open source application development framework for PHP 5.4 or newer
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
 * @license        http://circle-creative.com/products/o2system/license.html
 * @license        http://opensource.org/licenses/MIT	MIT License
 * @link           http://circle-creative.com
 * @since          Version 2.0
 * @filesource
 */
// --------------------------------------------------------------------

namespace O2System\O2DB\Drivers\Mysqli;

use O2System\O2DB\Factory\Manager;

defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );

// --------------------------------------------------------------------

/**
 * Mysqli Database Adapter Class
 *
 * Porting from CodeIgniter Database Mysqli Driver
 *
 * @package        O2System
 * @subpackage     Drivers
 * @category       Database
 * @author         EllisLab Dev Team
 *                 Circle Creative Dev Team
 * @link           http://o2system.center/wiki/#Database
 */
class Driver extends Manager
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'mysqli';

    /**
     * Compression flag
     *
     * @var    bool
     */
    public $compress = FALSE;

    /**
     * DELETE hack flag
     *
     * Whether to use the MySQL "delete hack" which allows the number
     * of affected rows to be shown. Uses a preg_replace when enabled,
     * adding a bit more processing to all queries.
     *
     * @var    bool
     */
    public $delete_hack = TRUE;

    /**
     * Strict ON flag
     *
     * Whether we're running in strict SQL mode.
     *
     * @var    bool
     */
    public $stricton = FALSE;

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * @var    string
     */
    protected $_escape_char = '`';

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param    bool $persistent
     *
     * @access  public
     *
     * @return    object
     * @todo    SSL support
     */
    public function db_connect( $persistent = FALSE )
    {
        // Do we have a socket path?
        if( $this->hostname[ 0 ] === '/' )
        {
            $hostname = NULL;
            $port = NULL;
            $socket = $this->hostname;
        }
        else
        {
            // Persistent connection support was added in PHP 5.3.0
            $hostname = ( $persistent === TRUE && is_php( '5.3' ) )
                ? 'p:' . $this->hostname : $this->hostname;
            $port = empty( $this->port ) ? NULL : $this->port;
            $socket = NULL;
        }

        $client_flags = ( $this->compress === TRUE ) ? MYSQLI_CLIENT_COMPRESS : 0;
        $mysqli = mysqli_init();

        $mysqli->options( MYSQLI_OPT_CONNECT_TIMEOUT, 10 );

        if( $this->stricton )
        {
            $mysqli->options( MYSQLI_INIT_COMMAND, 'SET SESSION sql_mode="STRICT_ALL_TABLES"' );
        }

        return $mysqli->real_connect( $hostname, $this->username, $this->password, $this->database, $port, $socket,
                                      $client_flags )
            ? $mysqli : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Reconnect
     *
     * Keep / reestablish the db connection if no queries have been
     * sent for a length of time exceeding the server's idle timeout
     *
     * @access public
     *
     * @return    void
     */
    public function reconnect()
    {
        if( $this->conn_id !== FALSE && $this->conn_id->ping() === FALSE )
        {
            $this->conn_id = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Select the database
     *
     * @access public
     *
     * @param    string $database
     *
     * @return    bool
     */
    public function db_select( $database = '' )
    {
        if( $database === '' )
        {
            $database = $this->database;
        }

        if( $this->conn_id->select_db( $database ) )
        {
            $this->database = $database;

            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Database version number
     *
     * @access public
     *
     * @return    string
     */
    public function version()
    {
        if( isset( $this->data_cache[ 'version' ] ) )
        {
            return $this->data_cache[ 'version' ];
        }

        return $this->data_cache[ 'version' ] = $this->conn_id->server_info;
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @access public
     *
     * @param    bool $test_mode
     *
     * @return    bool
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

        $this->conn_id->autocommit( FALSE );

        return is_php( '5.5' )
            ? $this->conn_id->begin_transaction()
            : $this->simple_query( 'START TRANSACTION' ); // can also be BEGIN or BEGIN WORK
    }

    // --------------------------------------------------------------------

    /**
     * Commit Transaction
     *
     * @access public
     *
     * @return    bool
     */
    public function trans_commit()
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        if( $this->conn_id->commit() )
        {
            $this->conn_id->autocommit( TRUE );

            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Rollback Transaction
     *
     * @access public
     *
     * @return    bool
     */
    public function trans_rollback()
    {
        // When transactions are nested we only begin/commit/rollback the outermost ones
        if( ! $this->trans_enabled OR $this->_trans_depth > 0 )
        {
            return TRUE;
        }

        if( $this->conn_id->rollback() )
        {
            $this->conn_id->autocommit( TRUE );

            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Affected Rows
     *
     * @access public
     *
     * @return    int
     */
    public function affected_rows()
    {
        return $this->conn_id->affected_rows;
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access public
     *
     * @return    int
     */
    public function insert_id()
    {
        return $this->conn_id->insert_id;
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @access public
     *
     * @param    string $table
     *
     * @return    array
     */
    public function field_data( $table )
    {
        if( ( $query = $this->query( 'SHOW COLUMNS FROM ' . $this->protect_identifiers( $table, TRUE, NULL,
                                                                                        FALSE ) ) ) === FALSE
        )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $retval = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $retval[ $i ] = new \stdClass();
            $retval[ $i ]->name = $query[ $i ]->Field;

            sscanf( $query[ $i ]->Type, '%[a-z](%d)',
                    $retval[ $i ]->type,
                    $retval[ $i ]->max_length
            );

            $retval[ $i ]->default = $query[ $i ]->Default;
            $retval[ $i ]->primary_key = (int)( $query[ $i ]->Key === 'PRI' );
        }

        return $retval;
    }

    // --------------------------------------------------------------------

    /**
     * Error
     *
     * Returns an array containing code and message of the last
     * database error that has occurred.
     *
     * @access public
     *
     * @return    array
     */
    public function error()
    {
        if( ! empty( $this->conn_id->connect_errno ) )
        {
            return array(
                'code'    => $this->conn_id->connect_errno,
                'message' => is_php( '5.2.9' ) ? $this->conn_id->connect_error : mysqli_connect_error()
            );
        }

        return array( 'code' => $this->conn_id->errno, 'message' => $this->conn_id->error );
    }

    // --------------------------------------------------------------------

    /**
     * Set client character set
     *
     * @access protected
     *
     * @param    string $charset
     *
     * @return    bool
     */
    protected function _db_set_charset( $charset )
    {
        return $this->conn_id->set_charset( $charset );
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @access protected
     *
     * @param    string $sql an SQL query
     *
     * @return    mixed
     */
    protected function _execute( $sql )
    {
        return $this->conn_id->query( $this->_prep_query( $sql ) );
    }

    // --------------------------------------------------------------------

    /**
     * Prep the query
     *
     * If needed, each database adapter can prep the query string
     *
     * @access protected
     *
     * @param    string $sql an SQL query
     *
     * @return    string
     */
    protected function _prep_query( $sql )
    {
        // mysqli_affected_rows() returns 0 for "DELETE FROM TABLE" queries. This hack
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
     * @access public
     *
     * @param    string
     *
     * @return    string
     */
    protected function _escape_str( $str )
    {
        return $this->conn_id->real_escape_string( $str );
    }

    // --------------------------------------------------------------------

    /**
     * List table query
     *
     * Generates a platform-specific query string so that the table names can be fetched
     *
     * @access public
     *
     * @param    bool $prefix_limit
     *
     * @return    string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = 'SHOW TABLES FROM ' . $this->escape_identifiers( $this->database );

        if( $prefix_limit !== FALSE && $this->db_prefix !== '' )
        {
            return $sql . " LIKE '" . $this->escape_like_str( $this->db_prefix ) . "%'";
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Show column query
     *
     * Generates a platform-specific query string so that the column names can be fetched
     *
     * @access public
     *
     * @param    string $table
     *
     * @return    string
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
     * @access protected
     *
     * @return    string
     */
    protected function _from_tables()
    {
        if( ! empty( $this->qb_join ) && count( $this->qb_from ) > 1 )
        {
            return '(' . implode( ', ', $this->qb_from ) . ')';
        }

        return implode( ', ', $this->qb_from );
    }

    // --------------------------------------------------------------------

    /**
     * Close DB Connection
     *
     * @access protected
     *
     * @return    void
     */
    protected function _close()
    {
        $this->conn_id->close();
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/Mysqli/Driver.php */
