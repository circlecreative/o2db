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

namespace O2System\O2DB\Drivers\Pdo\Dblib;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * PDO DBLIB Database Driver
 *
 * @author      Circle Creative Developer Team
 */
class Driver extends DriverInterface
{
    /**
     * Sub-driver
     *
     * @access  public
     * @type    string
     */
    public $sub_driver = 'dblib';

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
     * @access  protected
     * @type    bool
     */
    protected $_quoted_identifier;

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Builds the DSN if not already set.
     *
     * @param    array $params
     *
     * @access  public
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( empty( $this->dsn ) )
        {
            $this->dsn = $params[ 'sub_driver' ] . ':host=' . ( empty( $this->hostname ) ? '127.0.0.1' : $this->hostname );

            if( ! empty( $this->port ) )
            {
                $this->dsn .= ( DIRECTORY_SEPARATOR === '\\' ? ',' : ':' ) . $this->port;
            }

            empty( $this->database ) OR $this->dsn .= ';dbname=' . $this->database;
            empty( $this->charset ) OR $this->dsn .= ';charset=' . $this->charset;
            empty( $this->appname ) OR $this->dsn .= ';appname=' . $this->appname;
        }
        else
        {
            if( ! empty( $this->charset ) && strpos( $this->dsn, 'charset=', 6 ) === FALSE )
            {
                $this->dsn .= ';charset=' . $this->charset;
            }

            $this->sub_driver = 'dblib';
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param   bool    $persistent
     *
     * @access  public
     * @return  object
     */
    public function connect( $persistent = FALSE )
    {
        $this->id_connection = parent::connect( $persistent );

        if( ! is_object( $this->id_connection ) )
        {
            return $this->id_connection;
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
     * Returns an object with field data
     *
     * @param   string  $table
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
     * Show table query
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
        $sql = 'SELECT ' . $this->escape_identifiers( 'name' )
               . ' FROM ' . $this->escape_identifiers( 'sysobjects' )
               . ' WHERE ' . $this->escape_identifiers( 'type' ) . " = 'U'";

        if( $prefix_limit === TRUE && $this->prefix_table !== '' )
        {
            $sql .= ' && ' . $this->escape_identifiers( 'name' ) . " LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%' "
                    . sprintf( $this->_like_escape_string, $this->_like_escape_character );
        }

        return $sql . ' ORDER BY ' . $this->escape_identifiers( 'name' );
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
     * Delete statement
     *
     * Generates a platform-specific delete string from the supplied data
     *
     * @param   string  $table
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
     * @param   string  $sql    SQL Query
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
     * @param   string  $table  Table name
     * @param   array   $keys   INSERT keys
     * @param   array   $values INSERT values
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

        if($this->debug_enabled)
        {
            throw new \Exception('Unsupported feature of the database platform you are using.');
        }

        return FALSE;
    }

}