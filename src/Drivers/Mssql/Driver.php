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

namespace O2System\O2DB\Drivers\Mssql;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * Microsoft SQL Server Database Driver
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
    protected $_random_keywords = array( 'NEWID()', 'RAND(%d)' );

    /**
     * Quoted identifier flag
     *
     * Whether to use SQL-92 standard quoted identifier
     * (double quotes) or brackets for identifier escaping.
     *
     * @access  protected
     * @type    bool
     */
    protected $_quoted_identifier = TRUE;

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Appends the port number to the hostname, if needed.
     *
     * @param    array $params
     *
     * @access public
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
     * @param   bool $persistent
     *
     * @access  public
     * @return  resource
     * @throws  \Exception
     */
    public function connect( $persistent = FALSE )
    {
        $this->id_connection = ( $persistent )
            ? mssql_pconnect( $this->hostname, $this->username, $this->password )
            : mssql_connect( $this->hostname, $this->username, $this->password );

        if( ! $this->id_connection )
        {
            return FALSE;
        }

        // ----------------------------------------------------------------

        // Select the DB... assuming a database name is specified in the config file
        if( $this->database !== '' && ! $this->select() )
        {
            if( $this->debug_enabled )
            {
                throw new \Exception( 'Unable to select the specified database: ' . $this->database );
            }

            return FALSE;
        }

        // Determine how identifiers are escaped
        $query = $this->query( 'SELECT CASE WHEN (@@OPTIONS | 256) = @@OPTIONS THEN 1 ELSE 0 END AS qi' );
        $query = $query->row_array();
        $this->_quoted_identifier = empty( $query ) ? FALSE : (bool)$query[ 'qi' ];
        $this->_escape_character = ( $this->_quoted_identifier ) ? '"' : array( '[', ']' );

        return $this->id_connection;
    }

    // --------------------------------------------------------------------

    /**
     * Select the database
     *
     * @param   string $database
     *
     * @access  public
     * @return  bool
     */
    public function select( $database = '' )
    {
        if( $database === '' )
        {
            $database = $this->database;
        }

        // Note: Escaping is required in the event that the DB name
        // contains reserved characters.
        if( mssql_select_db( '[' . $database . ']', $this->id_connection ) )
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

        return $this->simple_query( 'BEGIN TRAN' );
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

        return $this->simple_query( 'COMMIT TRAN' );
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
        return mssql_rows_affected( $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * Returns the last id created in the Identity column.
     *
     * @access  public
     * @return  string
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
     * @param   string $table
     *
     * @access  public
     * @return  array
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
     * @access  public
     * @return  array
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
     * @param   string $sql An SQL query
     *
     * @access  protected
     * @return  mixed       resource if rows are returned, bool otherwise
     */
    protected function _execute( $sql )
    {
        return mssql_query( $sql, $this->id_connection );
    }

    // --------------------------------------------------------------------

    /**
     * Set client character set
     *
     * @param   string $charset
     *
     * @access  protected
     * @return  bool
     */
    protected function _set_charset( $charset )
    {
        return ( ini_set( 'mssql.charset', $charset ) !== FALSE );
    }

    // --------------------------------------------------------------------

    /**
     * Version number query string
     *
     * @access  protected
     * @return  string
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
     * @param   bool $prefix_limit
     *
     * @access  protected
     * @return  string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = 'SELECT ' . $this->escape_identifiers( 'name' )
               . ' FROM ' . $this->escape_identifiers( 'sysobjects' )
               . ' WHERE ' . $this->escape_identifiers( 'type' ) . " = 'U'";

        if( $prefix_limit !== FALSE && $this->prefix_table !== '' )
        {
            $sql .= ' && ' . $this->escape_identifiers( 'name' ) . " LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%' "
                    . sprintf( $this->_like_escape_string, $this->_like_escape_character );
        }

        return $sql . ' ORDER BY ' . $this->escape_identifiers( 'name' );
    }

    // --------------------------------------------------------------------

    /**
     * List column query
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
        return 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.Columns WHERE UPPER(TABLE_NAME) = ' . $this->escape( strtoupper( $table ) );
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
        return 'TRUNCATE TABLE ' . $table;
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
        if( $this->_limit )
        {
            return 'WITH o2db_delete AS (SELECT TOP ' . $this->_limit . ' * FROM ' . $table . $this->_compile_where( '_where' ) . ') DELETE FROM o2db_delete';
        }

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
        $limit = $this->_offset + $this->_limit;

        // As of SQL Server 2005 (9.0.*) ROW_NUMBER() is supported,
        // however an ORDER BY clause is required for it to work
        if( version_compare( $this->version(), '9', '>=' ) && $this->_offset && ! empty( $this->_order_by ) )
        {
            $order_by = $this->_compile_order_by();

            // We have to strip the ORDER BY clause
            $sql = trim( substr( $sql, 0, strrpos( $sql, $order_by ) ) );

            // Get the fields to select from our subquery, so that we can avoid O2DB_rownum appearing in the actual results
            if( count( $this->_select ) === 0 )
            {
                $select = '*'; // Inevitable
            }
            else
            {
                // Use only field names and their aliases, everything else is out of our scope.
                $select = array();
                $field_regexp = ( $this->_quoted_identifier )
                    ? '("[^\"]+")' : '(\[[^\]]+\])';
                for( $i = 0, $c = count( $this->_select ); $i < $c; $i++ )
                {
                    $select[ ] = preg_match( '/(?:\s|\.)' . $field_regexp . '$/i', $this->_select[ $i ], $m )
                        ? $m[ 1 ] : $this->_select[ $i ];
                }
                $select = implode( ', ', $select );
            }

            return 'SELECT ' . $select . " FROM (\n\n"
                   . preg_replace( '/^(SELECT( DISTINCT)?)/i', '\\1 ROW_NUMBER() OVER(' . trim( $order_by ) . ') AS ' . $this->escape_identifiers( 'O2DB_rownum' ) . ', ', $sql )
                   . "\n\n) " . $this->escape_identifiers( 'O2DB_subquery' )
                   . "\nWHERE " . $this->escape_identifiers( 'O2DB_rownum' ) . ' BETWEEN ' . ( $this->_offset + 1 ) . ' && ' . $limit;
        }

        return preg_replace( '/(^\SELECT (DISTINCT)?)/i', '\\1 TOP ' . $limit . ' ', $sql );
    }

    // --------------------------------------------------------------------

    /**
     * Insert batch statement
     *
     * Generates a platform-specific insert string from the supplied data.
     *
     * @param   string $table  Table name
     * @param   array  $keys   INSERT keys
     * @param   array  $values INSERT values
     *
     * @access  protected
     * @return  string|bool
     * @throws  \Exception
     */
    protected function _insert_batch( $table, $keys, $values )
    {
        // Multiple-value inserts are only supported as of SQL Server 2008
        if( version_compare( $this->version(), '10', '>=' ) )
        {
            return parent::_insert_batch( $table, $keys, $values );
        }

        if( $this->debug_enabled )
        {
            throw new \Exception( 'Unsupported feature of the database platform you are using.' );
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
        mssql_close( $this->id_connection );
    }

}
