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
namespace O2System\O2DB\Drivers\SQLite;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Driver extends \O2System\O2DB
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'sqlite';

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @var    array
     */
    protected $_random_keyword = array( 'RANDOM()', 'RANDOM()' );

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
        $error = NULL;
        $conn_id = ( $persistent === TRUE )
            ? sqlite_popen( $this->database, 0666, $error )
            : sqlite_open( $this->database, 0666, $error );

        isset( $error ) && log_message( 'error', $error );

        return $conn_id;
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
        return isset( $this->data_cache[ 'version' ] )
            ? $this->data_cache[ 'version' ]
            : $this->data_cache[ 'version' ] = sqlite_libversion();
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

        $this->simple_query( 'BEGIN TRANSACTION' );

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

        $this->simple_query( 'COMMIT' );

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

        $this->simple_query( 'ROLLBACK' );

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
        return sqlite_changes( $this->conn_id );
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
        return sqlite_last_insert_rowid( $this->conn_id );
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
        if( ( $query = $this->query( 'PRAGMA TABLE_INFO(' . $this->protect_identifiers( $table, TRUE, NULL, FALSE ) . ')' ) ) === FALSE )
        {
            return FALSE;
        }

        $query = $query->result_array();
        if( empty( $query ) )
        {
            return FALSE;
        }

        $retval = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $retval[ $i ] = new \stdClass();
            $retval[ $i ]->name = $query[ $i ][ 'name' ];
            $retval[ $i ]->type = $query[ $i ][ 'type' ];
            $retval[ $i ]->max_length = NULL;
            $retval[ $i ]->default = $query[ $i ][ 'dflt_value' ];
            $retval[ $i ]->primary_key = isset( $query[ $i ][ 'pk' ] ) ? (int)$query[ $i ][ 'pk' ] : 0;
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
        $error = array( 'code' => sqlite_last_error( $this->conn_id ) );
        $error[ 'message' ] = sqlite_error_string( $error[ 'code' ] );

        return $error;
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
        return $this->is_write_type( $sql )
            ? sqlite_exec( $this->conn_id, $sql )
            : sqlite_query( $this->conn_id, $sql );
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
        return sqlite_escape_string( $str );
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
        $sql = "SELECT name FROM sqlite_master WHERE type='table'";

        if( $prefix_limit !== FALSE && $this->db_prefix != '' )
        {
            return $sql . " && 'name' LIKE '" . $this->escape_like_str( $this->db_prefix ) . "%' " . sprintf( $this->_like_escape_str, $this->_like_escape_chr );
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
     * @return    bool
     */
    protected function _list_columns( $table = '' )
    {
        // Not supported
        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Replace statement
     *
     * Generates a platform-specific replace string from the supplied data
     *
     * @access protected
     *
     * @param    string $table  Table name
     * @param    array  $keys   INSERT keys
     * @param    array  $values INSERT values
     *
     * @return    string
     */
    protected function _replace( $table, $keys, $values )
    {
        return 'INSERT OR ' . parent::_replace( $table, $keys, $values );
    }

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the TRUNCATE statement,
     * then this function maps to 'DELETE FROM table'
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
     * Close DB Connection
     *
     * @access protected
     *
     * @return    void
     */
    protected function _close()
    {
        sqlite_close( $this->conn_id );
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/SQLite/Driver.php */
