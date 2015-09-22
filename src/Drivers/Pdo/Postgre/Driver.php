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

namespace O2System\O2DB\Drivers\Pdo\Postgre;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * PDO ODBC Database Driver
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
    public $sub_driver = 'pgsql';

    /**
     * Database schema
     *
     * @access  public
     * @type    string
     */
    public $schema = 'public';

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @access  public
     * @type    array
     */
    protected $_random_keyword = array( 'RANDOM()', 'RANDOM()' );

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Builds the DSN if not already set.
     *
     * @access  public
     *
     * @param   array $params
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( empty( $this->dsn ) )
        {
            $this->dsn = 'pgsql:host=' . ( empty( $this->hostname ) ? '127.0.0.1' : $this->hostname );

            empty( $this->port ) OR $this->dsn .= ';port=' . $this->port;
            empty( $this->database ) OR $this->dsn .= ';dbname=' . $this->database;

            if( ! empty( $this->username ) )
            {
                $this->dsn .= ';username=' . $this->username;
                empty( $this->password ) OR $this->dsn .= ';password=' . $this->password;
            }
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @param   bool $persistent
     *
     * @access  public
     * @return  object
     */
    public function connect( $persistent = FALSE )
    {
        $this->id_connection = parent::connect( $persistent );

        if( is_object( $this->id_connection ) && ! empty( $this->schema ) )
        {
            $this->simple_query( 'SET search_path TO ' . $this->schema . ',public' );
        }

        return $this->id_connection;
    }

    // --------------------------------------------------------------------

    /**
     * Insert ID
     *
     * @param   string $name
     *
     * @access  public
     * @return  int
     */
    public function insert_id( $name = NULL )
    {
        if( $name === NULL && version_compare( $this->version(), '8.1', '>=' ) )
        {
            $query = $this->query( 'SELECT LASTVAL() AS ins_id' );
            $query = $query->row();

            return $query->ins_id;
        }

        return $this->id_connection->lastInsertId( $name );
    }

    // --------------------------------------------------------------------

    /**
     * Determines if a query is a "write" type.
     *
     * @param   string $sql An SQL query string
     *
     * @access  public
     * @return  bool
     */
    public function is_write_type( $sql )
    {
        return (bool)preg_match( '/^\s*"?(SET|INSERT(?![^\)]+\)\s+RETURNING)|UPDATE(?!.*\sRETURNING)|DELETE|CREATE|DROP|TRUNCATE|LOAD|COPY|ALTER|RENAME|GRANT|REVOKE|LOCK|UNLOCK|REINDEX)\s/i', str_replace( array(
                                                                                                                                                                                                                 "\r\n",
                                                                                                                                                                                                                 "\r",
                                                                                                                                                                                                                 "\n"
                                                                                                                                                                                                             ), ' ', $sql ) );
    }

    // --------------------------------------------------------------------

    /**
     * ORDER BY
     *
     * @param   string  $order_by
     * @param   string  $direction  ASC, DESC or RANDOM
     * @param   bool    $escape
     *
     * @access  public
     * @return  object
     */
    public function order_by( $order_by, $direction = '', $escape = NULL )
    {
        $direction = strtoupper( trim( $direction ) );
        if( $direction === 'RANDOM' )
        {
            if( ! is_float( $order_by ) && ctype_digit( (string)$order_by ) )
            {
                $order_by = ( $order_by > 1 )
                    ? (float)'0.' . $order_by
                    : (float)$order_by;
            }

            if( is_float( $order_by ) )
            {
                $this->simple_query( 'SET SEED ' . $order_by );
            }

            $order_by = $this->_random_keyword[ 0 ];
            $direction = '';
            $escape = FALSE;
        }

        return parent::order_by( $order_by, $direction, $escape );
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
        $sql = 'SELECT "column_name", "data_type", "character_maximum_length", "numeric_precision", "column_default"
			FROM "information_schema"."columns"
			WHERE LOWER("table_name") = ' . $this->escape( strtolower( $table ) );

        if( ( $query = $this->query( $sql ) ) === FALSE )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $data = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $query[ $i ]->column_name;
            $data[ $i ]->type = $query[ $i ]->data_type;
            $data[ $i ]->max_length = ( $query[ $i ]->character_maximum_length > 0 ) ? $query[ $i ]->character_maximum_length : $query[ $i ]->numeric_precision;
            $data[ $i ]->default = $query[ $i ]->column_default;
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
        $sql = 'SELECT "table_name" FROM "information_schema"."tables" WHERE "table_schema" = \'' . $this->schema . "'";

        if( $prefix_limit === TRUE && $this->prefix_table !== '' )
        {
            return $sql . ' && "table_name" LIKE \''
                   . $this->escape_like_string( $this->prefix_table ) . "%' "
                   . sprintf( $this->_like_escape_string, $this->_like_escape_character );
        }

        return $sql;
    }

    // --------------------------------------------------------------------

    /**
     * List column query
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
        return 'SELECT "column_name"
			FROM "information_schema"."columns"
			WHERE LOWER("table_name") = ' . $this->escape( strtolower( $table ) );
    }

    // --------------------------------------------------------------------

    /**
     * "Smart" Escape String
     *
     * Escapes data based on type
     *
     * @param   string  $string
     *
     * @access  public
     * @return  mixed
     */
    public function escape( $string )
    {
        if( is_bool( $string ) )
        {
            return ( $string ) ? 'TRUE' : 'FALSE';
        }

        return parent::escape( $string );
    }

    // --------------------------------------------------------------------

    /**
     * Update statement
     *
     * Generates a platform-specific update string from the supplied data
     *
     * @param   string  $table
     * @param   array   $values
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
     * Update_Batch statement
     *
     * Generates a platform-specific batch update string from the supplied data
     *
     * @param   string  $table  Table name
     * @param   array   $values Update data
     * @param   string  $index  WHERE key
     *
     * @access  protected
     * @return  string
     */
    protected function _update_batch( $table, $values, $index )
    {
        $ids = array();
        foreach( $values as $key => $val )
        {
            $ids[ ] = $val[ $index ];

            foreach( array_keys( $val ) as $field )
            {
                if( $field !== $index )
                {
                    $final[ $field ][ ] = 'WHEN ' . $val[ $index ] . ' THEN ' . $val[ $field ];
                }
            }
        }

        $cases = '';
        foreach( $final as $k => $v )
        {
            $cases .= $k . ' = (CASE ' . $index . "\n"
                      . implode( "\n", $v ) . "\n"
                      . 'ELSE ' . $k . ' END), ';
        }

        $this->where( $index . ' IN(' . implode( ',', $ids ) . ')', NULL, FALSE );

        return 'UPDATE ' . $table . ' SET ' . substr( $cases, 0, -2 ) . $this->_compile_where( '_where' );
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
        $this->_limit = FALSE;

        return parent::_delete( $table );
    }

    // --------------------------------------------------------------------

    /**
     * LIMIT
     *
     * Generates a platform-specific LIMIT clause
     *
     * @param   string  $sql SQL Query
     *
     * @access  protected
     * @return  string
     */
    protected function _limit( $sql )
    {
        return $sql . ' LIMIT ' . $this->_limit . ( $this->_offset ? ' OFFSET ' . $this->_offset : '' );
    }

}
