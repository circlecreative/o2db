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

namespace O2System\O2DB\Drivers\Sqlite3;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * SQLite3 Database Driver
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
     * @param   bool $persistent
     *
     * @access  public
     * @return  SQLite3
     * @throws  \Exception
     */
    public function connect( $persistent = FALSE )
    {
        if( $persistent )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'SQLite3 doesn\'t support persistent connections' );
            }

            return FALSE;
        }

        try
        {
            $this->forge = new Forge( $this );
            $this->utility = new Utility( $this );

            return ( ! $this->password )
                ? new \SQLite3( $this->database )
                : new \SQLite3( $this->database, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, $this->password );
        }
        catch( \Exception $e )
        {
            throw new \Exception( $e->getMessage() );
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

        $version = \SQLite3::version();

        return $this->data_cache[ 'version' ] = $version[ 'versionString' ];
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

        return $this->id_connection->exec( 'BEGIN TRANSACTION' );
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

        return $this->id_connection->exec( 'END TRANSACTION' );
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

        return $this->id_connection->exec( 'ROLLBACK' );
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
        return $this->id_connection->changes();
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
        return $this->id_connection->lastInsertRowID();
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
        return array(
            'code' => $this->id_connection->lastErrorCode(), 'message' => $this->id_connection->lastErrorMsg()
        );
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @todo    Implement use of SQLite3::querySingle(), if needed
     *
     * @param   string $sql SQL Query
     *
     * @access  protected
     * @return  mixed SQLite3Result object or bool
     */
    protected function _execute( $sql )
    {
        return $this->is_write_type( $sql )
            ? $this->id_connection->exec( $sql )
            : $this->id_connection->query( $sql );
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
        return $this->id_connection->escapeString( $string );
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
        return 'SELECT "NAME" FROM "SQLITE_MASTER" WHERE "TYPE" = \'table\''
               . ( ( $prefix_limit !== FALSE && $this->prefix_table != '' )
            ? ' && "NAME" LIKE \'' . $this->escape_like_string( $this->prefix_table ) . '%\' ' . sprintf( $this->_like_escape_string, $this->_like_escape_character )
            : '' );
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
        // Not supported
        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Replace statement
     *
     * Generates a platform-specific replace string from the supplied data
     *
     * @param   string $table Table name
     * @param   array  $keys INSERT keys
     * @param   array  $values INSERT values
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
     * Close DB Connection
     *
     * @access  protected
     * @return  void
     */
    protected function _close()
    {
        $this->id_connection->close();
    }

}