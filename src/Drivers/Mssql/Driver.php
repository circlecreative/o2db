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
namespace O2System\O2DB\Drivers\MsSQL;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Driver extends \O2System\O2DB
{

    /**
     * Database driver
     *
     * @var    string
     */
    public $dbdriver = 'mssql';

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @var    array
     */
    protected $_random_keyword = array( 'NEWID()', 'RAND(%d)' );

    /**
     * Quoted identifier flag
     *
     * Whether to use SQL-92 standard quoted identifier
     * (double quotes) or brackets for identifier escaping.
     *
     * @var    bool
     */
    protected $_quoted_identifier = TRUE;

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Appends the port number to the hostname, if needed.
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

        if( ! empty( $this->port ) )
        {
            $this->hostname .= ( DIRECTORY_SEPARATOR === '\\' ? ',' : ':' ) . $this->port;
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
        $this->conn_id = ( $persistent )
            ? mssql_pconnect( $this->hostname, $this->username, $this->password )
            : mssql_connect( $this->hostname, $this->username, $this->password );

        if( ! $this->conn_id )
        {
            return FALSE;
        }

        // ----------------------------------------------------------------

        // Select the DB... assuming a database name is specified in the config file
        if( $this->database !== '' && ! $this->db_select() )
        {
            log_message( 'error', 'Unable to select database: ' . $this->database );

            return ( $this->db_debug === TRUE )
                ? $this->display_error( 'db_unable_to_select', $this->database )
                : FALSE;
        }

        // Determine how identifiers are escaped
        $query = $this->query( 'SELECT CASE WHEN (@@OPTIONS | 256) = @@OPTIONS THEN 1 ELSE 0 END AS qi' );
        $query = $query->row_array();
        $this->_quoted_identifier = empty( $query ) ? FALSE : (bool)$query[ 'qi' ];
        $this->_escape_char = ( $this->_quoted_identifier ) ? '"' : array( '[', ']' );

        return $this->conn_id;
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

        // Note: Escaping is required in the event that the DB name
        // contains reserved characters.
        if( mssql_select_db( '[' . $database . ']', $this->conn_id ) )
        {
            $this->database = $database;

            return TRUE;
        }

        return FALSE;
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

        return $this->simple_query( 'BEGIN TRAN' );
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

        return $this->simple_query( 'COMMIT TRAN' );
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

        return $this->simple_query( 'ROLLBACK TRAN' );
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
        return mssql_rows_affected( $this->conn_id );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * Returns the last id created in the Identity column.
     *
     * @access public
     *
     * @return    string
     */
    public function insert_id()
    {
        $query = version_compare( $this->version(), '8', '>=' )
            ? 'SELECT SCOPE_IDENTITY() AS last_id'
            : 'SELECT @@IDENTITY AS last_id';

        $query = $this->query( $query );
        $query = $query->row();

        return $query->last_id;
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
        $sql = 'SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, COLUMN_DEFAULT
			FROM INFORMATION_SCHEMA.Columns
			WHERE UPPER(TABLE_NAME) = ' . $this->escape( strtoupper( $table ) );

        if( ( $query = $this->query( $sql ) ) === FALSE )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $retval = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $retval[ $i ] = new \stdClass();
            $retval[ $i ]->name = $query[ $i ]->COLUMN_NAME;
            $retval[ $i ]->type = $query[ $i ]->DATA_TYPE;
            $retval[ $i ]->max_length = ( $query[ $i ]->CHARACTER_MAXIMUM_LENGTH > 0 ) ? $query[ $i ]->CHARACTER_MAXIMUM_LENGTH : $query[ $i ]->NUMERIC_PRECISION;
            $retval[ $i ]->default = $query[ $i ]->COLUMN_DEFAULT;
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
        $query = $this->query( 'SELECT @@ERROR AS code' );
        $query = $query->row();

        return array( 'code' => $query->code, 'message' => mssql_get_last_message() );
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @access protected
     *
     * @param    string $sql an SQL query
     *
     * @return    mixed    resource if rows are returned, bool otherwise
     */
    protected function _execute( $sql )
    {
        return mssql_query( $sql, $this->conn_id );
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
        return ( ini_set( 'mssql.charset', $charset ) !== FALSE );
    }

    // --------------------------------------------------------------------

    /**
     * Version number query string
     *
     * @access protected
     *
     * @return    string
     */
    protected function _version()
    {
        return 'SELECT @@VERSION AS ver';
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
        $sql = 'SELECT ' . $this->escape_identifiers( 'name' )
               . ' FROM ' . $this->escape_identifiers( 'sysobjects' )
               . ' WHERE ' . $this->escape_identifiers( 'type' ) . " = 'U'";

        if( $prefix_limit !== FALSE && $this->db_prefix !== '' )
        {
            $sql .= ' && ' . $this->escape_identifiers( 'name' ) . " LIKE '" . $this->escape_like_str( $this->db_prefix ) . "%' "
                    . sprintf( $this->_like_escape_str, $this->_like_escape_chr );
        }

        return $sql . ' ORDER BY ' . $this->escape_identifiers( 'name' );
    }

    // --------------------------------------------------------------------

    /**
     * List column query
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
        return 'SELECT COLUMN_NAME
			FROM INFORMATION_SCHEMA.Columns
			WHERE UPPER(TABLE_NAME) = ' . $this->escape( strtoupper( $table ) );
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
        return 'TRUNCATE TABLE ' . $table;
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
        if( $this->qb_limit )
        {
            return 'WITH ci_delete AS (SELECT TOP ' . $this->qb_limit . ' * FROM ' . $table . $this->_compile_wh( 'qb_where' ) . ') DELETE FROM ci_delete';
        }

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
        $limit = $this->qb_offset + $this->qb_limit;

        // As of SQL Server 2005 (9.0.*) ROW_NUMBER() is supported,
        // however an ORDER BY clause is required for it to work
        if( version_compare( $this->version(), '9', '>=' ) && $this->qb_offset && ! empty( $this->qb_orderby ) )
        {
            $orderby = $this->_compile_order_by();

            // We have to strip the ORDER BY clause
            $sql = trim( substr( $sql, 0, strrpos( $sql, $orderby ) ) );

            // Get the fields to select from our subquery, so that we can avoid CI_rownum appearing in the actual results
            if( count( $this->qb_select ) === 0 )
            {
                $select = '*'; // Inevitable
            }
            else
            {
                // Use only field names and their aliases, everything else is out of our scope.
                $select = array();
                $field_regexp = ( $this->_quoted_identifier )
                    ? '("[^\"]+")' : '(\[[^\]]+\])';
                for( $i = 0, $c = count( $this->qb_select ); $i < $c; $i++ )
                {
                    $select[ ] = preg_match( '/(?:\s|\.)' . $field_regexp . '$/i', $this->qb_select[ $i ], $m )
                        ? $m[ 1 ] : $this->qb_select[ $i ];
                }
                $select = implode( ', ', $select );
            }

            return 'SELECT ' . $select . " FROM (\n\n"
                   . preg_replace( '/^(SELECT( DISTINCT)?)/i', '\\1 ROW_NUMBER() OVER(' . trim( $orderby ) . ') AS ' . $this->escape_identifiers( 'CI_rownum' ) . ', ', $sql )
                   . "\n\n) " . $this->escape_identifiers( 'CI_subquery' )
                   . "\nWHERE " . $this->escape_identifiers( 'CI_rownum' ) . ' BETWEEN ' . ( $this->qb_offset + 1 ) . ' && ' . $limit;
        }

        return preg_replace( '/(^\SELECT (DISTINCT)?)/i', '\\1 TOP ' . $limit . ' ', $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Insert batch statement
     *
     * Generates a platform-specific insert string from the supplied data.
     *
     * @access protected
     *
     * @param    string $table  Table name
     * @param    array  $keys   INSERT keys
     * @param    array  $values INSERT values
     *
     * @return    string|bool
     */
    protected function _insert_batch( $table, $keys, $values )
    {
        // Multiple-value inserts are only supported as of SQL Server 2008
        if( version_compare( $this->version(), '10', '>=' ) )
        {
            return parent::_insert_batch( $table, $keys, $values );
        }

        return ( $this->db->db_debug ) ? $this->db->display_error( 'db_unsupported_feature' ) : FALSE;
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
        mssql_close( $this->conn_id );
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/MsSQL/Driver.php */
