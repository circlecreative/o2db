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

namespace O2System\O2DB\Drivers\Sqlite;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * SQLite Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * ORDER BY random keyword
     *
     * @access  protected
     * @type    array
     */
    protected $_random_keywords = array( 'RANDOM()', 'RANDOM()' );

    // --------------------------------------------------------------------

    /**
     * Non-persistent database connection
     *
     * @param   bool    $persistent
     *
     * @access  public
     * @return  resource
     * @throws  \Exception
     */
    public function connect( $persistent = FALSE )
    {
        $error = NULL;
        $conn_id = ( $persistent === TRUE )
            ? sqlite_popen( $this->database, 0666, $error )
            : sqlite_open( $this->database, 0666, $error );

        if(isset($error))
        {
            if($this->debug_enabled)
            {
                throw new \Exception($error);
            }
        }

        return $conn_id;
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
        return isset( $this->data_cache[ 'version' ] )
            ? $this->data_cache[ 'version' ]
            : $this->data_cache[ 'version' ] = sqlite_libversion();
    }

    // --------------------------------------------------------------------

    /**
     * Begin Transaction
     *
     * @param   bool    $test_mode
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

        $this->simple_query( 'BEGIN TRANSACTION' );

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
        return sqlite_changes( $this->id_connection );
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
        return sqlite_last_insert_rowid( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param   string  $table
     *
     * @access  public
     * @return  array
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

        $data = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $query[ $i ][ 'name' ];
            $data[ $i ]->type = $query[ $i ][ 'type' ];
            $data[ $i ]->max_length = NULL;
            $data[ $i ]->default = $query[ $i ][ 'dflt_value' ];
            $data[ $i ]->primary_key = isset( $query[ $i ][ 'pk' ] ) ? (int)$query[ $i ][ 'pk' ] : 0;
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
        $error = array( 'code' => sqlite_last_error( $this->id_connection ) );
        $error[ 'message' ] = sqlite_error_string( $error[ 'code' ] );

        return $error;
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param   string  $sql    An SQL query
     *
     * @access  protected
     * @return  resource
     */
    protected function _execute( $sql )
    {
        return $this->is_write_type( $sql )
            ? sqlite_exec( $this->id_connection, $sql )
            : sqlite_query( $this->id_connection, $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Platform-dependant string escape
     *
     * @param   string  $string
     *
     * @access  protected
     * @return  string
     */
    protected function _escape_string( $string )
    {
        return sqlite_escape_string( $string );
    }

    // --------------------------------------------------------------------

    /**
     * List table query
     *
     * Generates a platform-specific query string so that the table names can be fetched
     *
     * @param   bool    $prefix_limit
     *
     * @access  protected
     * @return  string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = "SELECT name FROM sqlite_master WHERE type='table'";

        if( $prefix_limit !== FALSE && $this->prefix_table != '' )
        {
            return $sql . " && 'name' LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%' " . sprintf( $this->_like_escape_string, $this->_like_escape_character );
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * Show column query
     *
     * Generates a platform-specific query string so that the column names can be fetched
     *
     * @param   string  $table
     *
     * @access  protected
     * @return  bool
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
     * @param   string  $table  Table name
     * @param   array   $keys   INSERT keys
     * @param   array   $values INSERT values
     *
     * @access  protected
     * @return  string
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
     * @param   string  $table
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
     * Close DB Connection
     *
     * @access  protected
     * @return  void
     */
    protected function _close()
    {
        sqlite_close( $this->id_connection );
    }

}
