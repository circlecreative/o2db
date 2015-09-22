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

namespace O2System\O2DB\Drivers\Interbase;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * Firebird/Interbase Database Driver
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
    protected $_random_keywords = array( 'RAND()', 'RAND()' );

    /**
     * IBase Transaction status flag
     *
     * @access  protected
     * @type    resource
     */
    protected $_ibase_trans;

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
            ? ibase_pconnect( $this->hostname . ':' . $this->database, $this->username, $this->password, $this->charset )
            : ibase_connect( $this->hostname . ':' . $this->database, $this->username, $this->password, $this->charset );
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

        $this->_ibase_trans = ibase_trans( $this->id_connection );

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
        if( ! $this->trans_enabled OR $this->_trans->depth > 0 )
        {
            return TRUE;
        }

        return ibase_commit( $this->_ibase_trans );
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

        return ibase_rollback( $this->_ibase_trans );
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
        return ibase_affected_rows( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @param   string $generator_name
     * @param   int    $inc_by
     *
     * @access  public
     * @return  int
     */
    public function insert_id( $generator_name, $inc_by = 0 )
    {
        //If a generator hasn't been used before it will return 0
        return ibase_gen_id( '"' . $generator_name . '"', $inc_by );
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
        $sql = 'SELECT "rfields"."RDB$FIELD_NAME" AS "name",
				CASE "fields"."RDB$FIELD_TYPE"
					WHEN 7 THEN \'SMALLINT\'
					WHEN 8 THEN \'INTEGER\'
					WHEN 9 THEN \'QUAD\'
					WHEN 10 THEN \'FLOAT\'
					WHEN 11 THEN \'DFLOAT\'
					WHEN 12 THEN \'DATE\'
					WHEN 13 THEN \'TIME\'
					WHEN 14 THEN \'CHAR\'
					WHEN 16 THEN \'INT64\'
					WHEN 27 THEN \'DOUBLE\'
					WHEN 35 THEN \'TIMESTAMP\'
					WHEN 37 THEN \'VARCHAR\'
					WHEN 40 THEN \'CSTRING\'
					WHEN 261 THEN \'BLOB\'
					ELSE NULL
				END AS "type",
				"fields"."RDB$FIELD_LENGTH" AS "max_length",
				"rfields"."RDB$DEFAULT_VALUE" AS "default"
			FROM "RDB$RELATION_FIELDS" "rfields"
				JOIN "RDB$FIELDS" "fields" ON "rfields"."RDB$FIELD_SOURCE" = "fields"."RDB$FIELD_NAME"
			WHERE "rfields"."RDB$RELATION_NAME" = ' . $this->escape( $table ) . '
			ORDER BY "rfields"."RDB$FIELD_POSITION"';

        return ( ( $query = $this->query( $sql ) ) !== FALSE )
            ? $query->result_object()
            : FALSE;
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
        return array( 'code' => ibase_errcode(), 'message' => ibase_errmsg() );
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
        return ibase_query( $this->id_connection, $sql );
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
        $sql = 'SELECT TRIM("RDB$RELATION_NAME") AS TABLE_NAME FROM "RDB$RELATIONS" WHERE "RDB$RELATION_NAME" NOT LIKE \'RDB$%\' && "RDB$RELATION_NAME" NOT LIKE \'MON$%\'';

        if( $prefix_limit !== FALSE && $this->prefix_table !== '' )
        {
            return $sql . ' && TRIM("RDB$RELATION_NAME") AS TABLE_NAME LIKE \'' . $this->escape_like_string( $this->prefix_table ) . "%' "
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
        return 'SELECT TRIM("RDB$FIELD_NAME") AS COLUMN_NAME FROM "RDB$RELATION_FIELDS" WHERE "RDB$RELATION_NAME" = ' . $this->escape( $table );
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
     * LIMIT
     *
     * Generates a platform-specific LIMIT clause
     *
     * @param   string $sql SQL Query
     *
     * @access  protected
     * @return  string
     */
    protected function _limit( $sql )
    {
        // Limit clause depends on if Interbase or Firebird
        if( stripos( $this->version(), 'firebird' ) !== FALSE )
        {
            $select = 'FIRST ' . $this->_limit
                      . ( $this->_offset ? ' SKIP ' . $this->_offset : '' );
        }
        else
        {
            $select = 'ROWS '
                      . ( $this->_offset ? $this->_offset . ' TO ' . ( $this->_limit + $this->_offset ) : $this->_limit );
        }

        return preg_replace( '`SELECT`i', 'SELECT ' . $select, $sql, 1 );
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

        if( ( $service = ibase_service_attach( $this->hostname, $this->username, $this->password ) ) )
        {
            $this->data_cache[ 'version' ] = ibase_server_info( $service, IBASE_SVC_SERVER_VERSION );

            // Don't keep the service open
            ibase_service_detach( $service );

            return $this->data_cache[ 'version' ];
        }

        return FALSE;
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
        ibase_close( $this->id_connection );
    }

}
