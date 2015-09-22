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

namespace O2System\O2DB\Drivers\Cubrid;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * Cubrid Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * Auto-commit flag
     *
     * @access  public
     * @type    bool
     */
    public $auto_commit = TRUE;

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * @access  protected
     * @type    string
     */
    protected $_escape_character = '`';

    /**
     * ORDER BY random keyword
     *
     * @access  protected
     * @type    array
     */
    protected $_random_keywords = array( 'RANDOM()', 'RANDOM(%d)' );

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
     * @param   bool $persistent
     *
     * @access  public
     * @return  resource
     */
    public function connect( $persistent = FALSE )
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
     * @access  public
     * @return  void
     */
    public function reconnect()
    {
        if( cubrid_ping( $this->id_connection ) === FALSE )
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

        return ( ! $this->id_connection OR ( $version = cubrid_get_server_info( $this->id_connection ) ) === FALSE )
            ? FALSE
            : $this->data_cache[ 'version' ] = $version;
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

        if( cubrid_get_autocommit( $this->id_connection ) )
        {
            cubrid_set_autocommit( $this->id_connection, CUBRID_AUTOCOMMIT_FALSE );
        }

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

        cubrid_commit( $this->id_connection );

        if( $this->auto_commit && ! cubrid_get_autocommit( $this->id_connection ) )
        {
            cubrid_set_autocommit( $this->id_connection, CUBRID_AUTOCOMMIT_TRUE );
        }

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

        cubrid_rollback( $this->id_connection );

        if( $this->auto_commit && ! cubrid_get_autocommit( $this->id_connection ) )
        {
            cubrid_set_autocommit( $this->id_connection, CUBRID_AUTOCOMMIT_TRUE );
        }

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
        return cubrid_affected_rows();
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
        return cubrid_insert_id( $this->id_connection );
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
        if( ( $query = $this->query( 'SHOW COLUMNS FROM ' . $this->protect_identifiers( $table, TRUE, NULL, FALSE ) ) ) === FALSE )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $field = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $field[ $i ] = new \stdClass();
            $field[ $i ]->name = $query[ $i ]->Field;

            sscanf( $query[ $i ]->Type, '%[a-z](%d)',
                    $field[ $i ]->type,
                    $field[ $i ]->max_length
            );

            $field[ $i ]->default = $query[ $i ]->Default;
            $field[ $i ]->primary_key = (int)( $query[ $i ]->Key === 'PRI' );
        }

        return $field;
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
        return array(
            'code' => cubrid_errno( $this->id_connection ), 'message' => cubrid_error( $this->id_connection )
        );
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param   string $sql an SQL query
     *
     * @access  protected
     * @return  resource
     */
    protected function _execute( $sql )
    {
        return cubrid_query( $sql, $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @param   string $string
     *
     * @access  public
     * @return  string
     */
    protected function _escape_string( $string )
    {
        return cubrid_real_escape_string( $string, $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * List table query
     *
     * Generates a platform-specific query string so that the table names can be fetched
     *
     * @param   bool $prefix_limit
     *
     * @access protected
     * @return  string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = 'SHOW TABLES';

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
        cubrid_close( $this->id_connection );
    }

}
