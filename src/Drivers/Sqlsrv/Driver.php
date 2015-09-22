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

namespace O2System\O2DB\Drivers\Sqlsrv;

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
     * Scrollable flag
     *
     * Determines what cursor type to use when executing queries.
     *
     * FALSE or SQLSRV_CURSOR_FORWARD would increase performance,
     * but would disable num_rows() (and possibly insert_id())
     *
     * @access  public
     * @type    mixed
     */
    public $scrollable;

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @access  public
     * @type    array
     */
    protected $_random_keywords = array( 'NEWID()', 'RAND(%d)' );

    /**
     * Quoted identifier flag
     *
     * Whether to use SQL-92 standard quoted identifier
     * (double quotes) or brackets for identifier escaping.
     *
     * @access  public
     * @type    bool
     */
    protected $_quoted_identifier = TRUE;

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

        // This is only supported as of SQLSRV 3.0
        if( $this->scrollable === NULL )
        {
            $this->scrollable = defined( 'SQLSRV_CURSOR_CLIENT_BUFFERED' )
                ? SQLSRV_CURSOR_CLIENT_BUFFERED
                : FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param   bool $pooling
     *
     * @access  public
     * @return  resource
     */
    public function connect( $pooling = FALSE )
    {
        $charset = in_array( strtolower( $this->charset ), array( 'utf-8', 'utf8' ), TRUE )
            ? 'UTF-8' : SQLSRV_ENC_CHAR;

        $connection = array(
            'UID'                  => empty( $this->username ) ? '' : $this->username,
            'PWD'                  => empty( $this->password ) ? '' : $this->password,
            'Database'             => $this->database,
            'ConnectionPooling'    => ( $pooling === TRUE ) ? 1 : 0,
            'CharacterSet'         => $charset,
            'Encrypt'              => ( $this->encrypt === TRUE ) ? 1 : 0,
            'ReturnDatesAsStrings' => 1
        );

        // If the username and password are both empty, assume this is a
        // 'Windows Authentication Mode' connection.
        if( empty( $connection[ 'UID' ] ) && empty( $connection[ 'PWD' ] ) )
        {
            unset( $connection[ 'UID' ], $connection[ 'PWD' ] );
        }

        $this->id_connection = sqlsrv_connect( $this->hostname, $connection );

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

        if( $this->_execute( 'USE ' . $this->escape_identifiers( $database ) ) )
        {
            $this->database = $database;

            return TRUE;
        }

        return FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Execute the query
     *
     * @param   string $sql An SQL query
     *
     * @access  public
     * @return  resource
     */
    protected function _execute( $sql )
    {
        return ( $this->scrollable === FALSE OR $this->is_write_type( $sql ) )
            ? sqlsrv_query( $this->id_connection, $sql )
            : sqlsrv_query( $this->id_connection, $sql, NULL, array( 'Scrollable' => $this->scrollable ) );
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

        return sqlsrv_begin_transaction( $this->id_connection );
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

        return sqlsrv_commit( $this->id_connection );
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

        return sqlsrv_rollback( $this->id_connection );
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
        return sqlsrv_rows_affected( $this->id_result );
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
        $query = $this->query( 'SELECT @@IDENTITY AS insert_id' );
        $query = $query->row();

        return $query->insert_id;
    }

    // --------------------------------------------------------------------

    /**
     * Returns an object with field data
     *
     * @param   string $table
     *
     * @access  protected
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

        $data = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $query[ $i ]->COLUMN_NAME;
            $data[ $i ]->type = $query[ $i ]->DATA_TYPE;
            $data[ $i ]->max_length = ( $query[ $i ]->CHARACTER_MAXIMUM_LENGTH > 0 ) ? $query[ $i ]->CHARACTER_MAXIMUM_LENGTH : $query[ $i ]->NUMERIC_PRECISION;
            $data[ $i ]->default = $query[ $i ]->COLUMN_DEFAULT;
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
        $error = array( 'code' => '00000', 'message' => '' );
        $sqlsrv_errors = sqlsrv_errors( SQLSRV_ERR_ERRORS );

        if( ! is_array( $sqlsrv_errors ) )
        {
            return $error;
        }

        $sqlsrv_error = array_shift( $sqlsrv_errors );
        if( isset( $sqlsrv_error[ 'SQLSTATE' ] ) )
        {
            $error[ 'code' ] = isset( $sqlsrv_error[ 'code' ] ) ? $sqlsrv_error[ 'SQLSTATE' ] . '/' . $sqlsrv_error[ 'code' ] : $sqlsrv_error[ 'SQLSTATE' ];
        }
        elseif( isset( $sqlsrv_error[ 'code' ] ) )
        {
            $error[ 'code' ] = $sqlsrv_error[ 'code' ];
        }

        if( isset( $sqlsrv_error[ 'message' ] ) )
        {
            $error[ 'message' ] = $sqlsrv_error[ 'message' ];
        }

        return $error;
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

        if( $prefix_limit === TRUE && $this->prefix_table !== '' )
        {
            $sql .= ' && ' . $this->escape_identifiers( 'name' ) . " LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%' "
                    . sprintf( $this->_escape_like_str, $this->_escape_like_chr );
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
        // As of SQL Server 2012 (11.0.*) OFFSET is supported
        if( version_compare( $this->version(), '11', '>=' ) )
        {
            // SQL Server OFFSET-FETCH can be used only with the ORDER BY clause
            empty( $this->_order_by ) && $sql .= ' ORDER BY 1';

            return $sql . ' OFFSET ' . (int)$this->_offset . ' ROWS FETCH NEXT ' . $this->_limit . ' ROWS ONLY';
        }

        $limit = $this->_offset + $this->_limit;

        // An ORDER BY clause is required for ROW_NUMBER() to work
        if( $this->_offset && ! empty( $this->_order_by ) )
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

        if( ! $this->id_connection OR ( $info = sqlsrv_server_info( $this->id_connection ) ) === FALSE )
        {
            return FALSE;
        }

        return $this->data_cache[ 'version' ] = $info[ 'SQLServerVersion' ];
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
        sqlsrv_close( $this->id_connection );
    }

}