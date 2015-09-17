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

namespace O2System;

// ------------------------------------------------------------------------

/**
 * Database Driver Class
 *
 * @package        O2System
 * @subpackage     Drivers
 * @category       Database
 * @author         Steeven Andrian Salim
 * @link           http://o2system.center/framework/user-guide/libraries/database.htm
 */
class Driver extends \O2System\O2DB
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'cubrid';

    /**
     * Auto-commit flag
     *
     * @var    bool
     */
    public $auto_commit = TRUE;

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * @var    string
     */
    protected $_escape_char = '`';

    /**
     * ORDER BY random keyword
     *
     * @var    array
     */
    protected $_random_keyword = array( 'RANDOM()', 'RANDOM(%d)' );

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @access public
     *
     * @param    array $params
     *
     * @return    void
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( preg_match( '/^CUBRID:[^:]+(:[0-9][1-9]{0,4})?:[^:]+:[^:]*:[^:]*:(\?.+)?$/', $this->dsn, $matches ) )
        {
            if( stripos( $matches[ 2 ], 'autocommit=off' ) !== FALSE )
            {
                $this->auto_commit = FALSE;
            }
        }
        else
        {
            // If no port is defined by the user, use the default value
            empty( $this->port ) OR $this->port = 33000;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Non-persistent database connection
     *
     * @access public
     *
     * @param    bool $persistent
     *
     * @return    resource
     */
    public function db_connect( $persistent = FALSE )
    {
        if( preg_match( '/^CUBRID:[^:]+(:[0-9][1-9]{0,4})?:[^:]+:([^:]*):([^:]*):(\?.+)?$/', $this->dsn, $matches ) )
        {
            $func = ( $persistent !== TRUE ) ? 'cubrid_connect_with_url' : 'cubrid_pconnect_with_url';

            return ( $matches[ 2 ] === '' && $matches[ 3 ] === '' && $this->username !== '' && $this->password !== '' )
                ? $func( $this->dsn, $this->username, $this->password )
                : $func( $this->dsn );
        }

        $func = ( $persistent !== TRUE ) ? 'cubrid_connect' : 'cubrid_pconnect';

        return ( $this->username !== '' )
            ? $func( $this->hostname, $this->port, $this->database, $this->username, $this->password )
            : $func( $this->hostname, $this->port, $this->database );
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
        if( cubrid_ping( $this->conn_id ) === FALSE )
        {
            $this->conn_id = FALSE;
        }
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

        return ( ! $this->conn_id OR ( $version = cubrid_get_server_info( $this->conn_id ) ) === FALSE )
            ? FALSE
            : $this->data_cache[ 'version' ] = $version;
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

        if( cubrid_get_autocommit( $this->conn_id ) )
        {
            cubrid_set_autocommit( $this->conn_id, CUBRID_AUTOCOMMIT_FALSE );
        }

        return TRUE;
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

        cubrid_commit( $this->conn_id );

        if( $this->auto_commit && ! cubrid_get_autocommit( $this->conn_id ) )
        {
            cubrid_set_autocommit( $this->conn_id, CUBRID_AUTOCOMMIT_TRUE );
        }

        return TRUE;
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

        cubrid_rollback( $this->conn_id );

        if( $this->auto_commit && ! cubrid_get_autocommit( $this->conn_id ) )
        {
            cubrid_set_autocommit( $this->conn_id, CUBRID_AUTOCOMMIT_TRUE );
        }

        return TRUE;
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
        return cubrid_affected_rows();
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
        return cubrid_insert_id( $this->conn_id );
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
        if( ( $query = $this->query( 'SHOW COLUMNS FROM ' . $this->protect_identifiers( $table, TRUE, NULL, FALSE ) ) ) === FALSE )
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
     * database error that has occured.
     *
     * @access public
     *
     * @return    array
     */
    public function error()
    {
        return array( 'code' => cubrid_errno( $this->conn_id ), 'message' => cubrid_error( $this->conn_id ) );
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @access protected
     *
     * @param    string $sql an SQL query
     *
     * @return    resource
     */
    protected function _execute( $sql )
    {
        return cubrid_query( $sql, $this->conn_id );
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
        return cubrid_real_escape_string( $str, $this->conn_id );
    }

    // --------------------------------------------------------------------

    /**
     * List table query
     *
     * Generates a platform-specific query string so that the table names can be fetched
     *
     * @access protected
     *
     * @param    bool $prefix_limit
     *
     * @return    string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = 'SHOW TABLES';

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
     * @access protected
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
        cubrid_close( $this->conn_id );
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/Cubrid/Driver.php */
