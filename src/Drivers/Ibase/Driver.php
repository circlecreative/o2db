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
namespace O2System\O2DB\Drivers\iBase;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );

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
     * @access public
     *
     * @var    string
     */
    public $dbdriver = 'ibase';

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @access protected
     *
     * @var    array
     */
    protected $_random_keyword = array( 'RAND()', 'RAND()' );

    /**
     * IBase Transaction status flag
     *
     * @access protected
     *
     * @var    resource
     */
    protected $_ibase_trans;

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
            ? ibase_pconnect( $this->hostname . ':' . $this->database, $this->username, $this->password, $this->charset )
            : ibase_connect( $this->hostname . ':' . $this->database, $this->username, $this->password, $this->charset );
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

        $this->_ibase_trans = ibase_trans( $this->conn_id );

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

        return ibase_rollback( $this->_ibase_trans );
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
        return ibase_affected_rows( $this->conn_id );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @access public
     *
     * @param    string $generator_name
     * @param    int    $inc_by
     *
     * @return    int
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
     * @access public
     *
     * @param    string $table
     *
     * @return    array
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
     * @access public
     *
     * @return    array
     */
    public function error()
    {
        return array( 'code' => ibase_errcode(), 'message' => ibase_errmsg() );
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
        return ibase_query( $this->conn_id, $sql );
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
        $sql = 'SELECT TRIM("RDB$RELATION_NAME") AS TABLE_NAME FROM "RDB$RELATIONS" WHERE "RDB$RELATION_NAME" NOT LIKE \'RDB$%\' && "RDB$RELATION_NAME" NOT LIKE \'MON$%\'';

        if( $prefix_limit !== FALSE && $this->db_prefix !== '' )
        {
            return $sql . ' && TRIM("RDB$RELATION_NAME") AS TABLE_NAME LIKE \'' . $this->escape_like_str( $this->db_prefix ) . "%' "
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
        return 'SELECT TRIM("RDB$FIELD_NAME") AS COLUMN_NAME FROM "RDB$RELATION_FIELDS" WHERE "RDB$RELATION_NAME" = ' . $this->escape( $table );
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
     * LIMIT
     *
     * Generates a platform-specific LIMIT clause
     *
     * @access protected
     *
     * @param    string $sql SQL Query
     *
     * @return    string
     */
    protected function _limit( $sql )
    {
        // Limit clause depends on if Interbase or Firebird
        if( stripos( $this->version(), 'firebird' ) !== FALSE )
        {
            $select = 'FIRST ' . $this->qb_limit
                      . ( $this->qb_offset ? ' SKIP ' . $this->qb_offset : '' );
        }
        else
        {
            $select = 'ROWS '
                      . ( $this->qb_offset ? $this->qb_offset . ' TO ' . ( $this->qb_limit + $this->qb_offset ) : $this->qb_limit );
        }

        return preg_replace( '`SELECT`i', 'SELECT ' . $select, $sql, 1 );
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
     * @access protected
     *
     * @return    void
     */
    protected function _close()
    {
        ibase_close( $this->conn_id );
    }

}

/* End of file Driver.php */
/* Location: ./system/libraries/iBase/Drivers.php */
