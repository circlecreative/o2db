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
namespace O2System\O2DB\Drivers\ODBC;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Driver extends \O2System\O2DB
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'odbc';

    /**
     * Database schema
     *
     * @var    string
     */
    public $schema = 'public';

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * Must be empty for ODBC.
     *
     * @var    string
     */
    protected $_escape_char = '';

    /**
     * ESCAPE statement string
     *
     * @var    string
     */
    protected $_like_escape_str = " {escape '%s'} ";

    /**
     * ORDER BY random keyword
     *
     * @var    array
     */
    protected $_random_keyword = array( 'RND()', 'RND(%d)' );

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
     * @access public
     *
     * @param    bool $persistent
     *
     * @return    resource
     */
    public function db_connect( $persistent = FALSE )
    {
        return ( $persistent === TRUE )
            ? odbc_pconnect( $this->dsn, $this->username, $this->password )
            : odbc_connect( $this->dsn, $this->username, $this->password );
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

        return odbc_autocommit( $this->conn_id, FALSE );
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

        $ret = odbc_commit( $this->conn_id );
        odbc_autocommit( $this->conn_id, TRUE );

        return $ret;
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

        $ret = odbc_rollback( $this->conn_id );
        odbc_autocommit( $this->conn_id, TRUE );

        return $ret;
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
        return odbc_num_rows( $this->result_id );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access public
     *
     * @return    bool
     */
    public function insert_id()
    {
        return ( $this->db->db_debug ) ? $this->db->display_error( 'db_unsupported_feature' ) : FALSE;
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
        return array( 'code' => odbc_error( $this->conn_id ), 'message' => odbc_errormsg( $this->conn_id ) );
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
        return odbc_exec( $this->conn_id, $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @access protected
     *
     * @param    string
     *
     * @return    string
     */
    protected function _escape_str( $str )
    {
        return remove_invisible_characters( $str );
    }

    // --------------------------------------------------------------------

    /**
     * Show table query
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
        $sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" . $this->schema . "'";

        if( $prefix_limit !== FALSE && $this->db_prefix !== '' )
        {
            return $sql . " && table_name LIKE '" . $this->escape_like_str( $this->db_prefix ) . "%' "
                   . sprintf( $this->_like_escape_str, $this->_like_escape_chr );
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
        return 'SHOW COLUMNS FROM ' . $table;
    }

    // --------------------------------------------------------------------

    /**
     * Field data query
     *
     * Generates a platform-specific query so that the column data can be retrieved
     *
     * @access protected
     *
     * @param    string $table
     *
     * @return    string
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
     * @access protected
     *
     * @param    string $table
     * @param    array  $values
     *
     * @return    string
     */
    protected function _update( $table, $values )
    {
        $this->qb_limit = FALSE;
        $this->qb_orderby = array();

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
     * @access protected
     *
     * @param    string $table
     *
     * @return    string
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
     * @access protected
     *
     * @param    string $table
     *
     * @return    string
     */
    protected function _delete( $table )
    {
        $this->qb_limit = FALSE;

        return parent::_delete( $table );
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
        odbc_close( $this->conn_id );
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/ODBC/Driver.php */
