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

namespace O2System\O2DB\Drivers\Odbc;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * ODBC (Unified) Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * Database schema
     *
     * @type    string
     */
    public $schema = 'public';

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * Must be empty for ODBC.
     *
     * @type    string
     */
    protected $_escape_character = '';

    /**
     * ESCAPE statement string
     *
     * @type    string
     */
    protected $_like_escape_string = " {escape '%s'} ";

    /**
     * ORDER BY random keyword
     *
     * @type    array
     */
    protected $_random_keyword = array( 'RND()', 'RND(%d)' );

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

        // Legacy support for DSN in the hostname field
        if( empty( $this->dsn ) )
        {
            $this->dsn = $this->hostname;
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
        return ( $persistent === TRUE )
            ? odbc_pconnect( $this->dsn, $this->username, $this->password )
            : odbc_connect( $this->dsn, $this->username, $this->password );
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

        return odbc_autocommit( $this->id_connection, FALSE );
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

        $return = odbc_commit( $this->id_connection );
        odbc_autocommit( $this->id_connection, TRUE );

        return $return;
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

        $return = odbc_rollback( $this->id_connection );
        odbc_autocommit( $this->id_connection, TRUE );

        return $return;
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
        return odbc_num_rows( $this->id_result );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access  public
     * @return  bool
     * @throws  \Exception
     */
    public function insert_id()
    {
        if( $this->debug_enabled )
        {
            throw new \Exception( 'Unsupported feature of the database platform you are using.' );
        }

        return FALSE;
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
            'code' => odbc_error( $this->id_connection ), 'message' => odbc_errormsg( $this->id_connection )
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
        return odbc_exec( $this->id_connection, $sql );
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
        return remove_invisible_characters( $string );
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
        $sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" . $this->schema . "'";

        if( $prefix_limit !== FALSE && $this->prefix_table !== '' )
        {
            return $sql . " && table_name LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%' "
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
        return 'SHOW COLUMNS FROM ' . $table;
    }

    // --------------------------------------------------------------------

    /**
     * Field data query
     *
     * Generates a platform-specific query so that the column data can be retrieved
     *
     * @param   string $table
     *
     * @access  protected
     * @return  string
     */
    protected function _field_data( $table )
    {
        return 'SELECT TOP 1 FROM ' . $table;
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
        return 'DELETE FROM ' . $table;
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
     * Close DB Connection
     *
     * @access  protected
     * @return  void
     */
    protected function _close()
    {
        odbc_close( $this->id_connection );
    }

}
