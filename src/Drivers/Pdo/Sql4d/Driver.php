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

namespace O2System\O2DB\Drivers\Pdo\Sql4d;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * PDO 4D Database Driver
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
    public $sub_driver = '4d';

    /**
     * Identifier escape character
     *
     * @access  protected
     * @type    string[]
     */
    protected $_escape_character = array( '[', ']' );

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Builds the DSN if not already set.
     *
     * @param   array $params
     *
     * @access  public
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( empty( $this->dsn ) )
        {
            $this->dsn = '4D:host=' . ( empty( $this->hostname ) ? '127.0.0.1' : $this->hostname );

            empty( $this->port ) OR $this->dsn .= ';port=' . $this->port;
            empty( $this->database ) OR $this->dsn .= ';dbname=' . $this->database;
            empty( $this->charset ) OR $this->dsn .= ';charset=' . $this->charset;
        }
        elseif( ! empty( $this->charset ) && strpos( $this->dsn, 'charset=', 3 ) === FALSE )
        {
            $this->dsn .= ';charset=' . $this->charset;
        }
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
        $sql = 'SELECT ' . $this->escape_identifiers( 'TABLE_NAME' ) . ' FROM ' . $this->escape_identifiers( '_USER_TABLES' );

        if( $prefix_limit === TRUE && $this->prefix_table !== '' )
        {
            $sql .= ' WHERE ' . $this->escape_identifiers( 'TABLE_NAME' ) . " LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%' "
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
        return 'SELECT ' . $this->escape_identifiers( 'COLUMN_NAME' ) . ' FROM ' . $this->escape_identifiers( '_USER_COLUMNS' )
               . ' WHERE ' . $this->escape_identifiers( 'TABLE_NAME' ) . ' = ' . $this->escape( $table );
    }

    // --------------------------------------------------------------------

    /**
     * Field data query
     *
     * Generates a platform-specific query so that the column data can be retrieved
     *
     * @param   string $table
     *
     * @access  protected
     * @return  string
     */
    protected function _field_data( $table )
    {
        return 'SELECT * FROM ' . $this->protect_identifiers( $table, TRUE, NULL, FALSE ) . ' LIMIT 1';
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
        return $sql . ' LIMIT ' . $this->_limit . ( $this->_offset ? ' OFFSET ' . $this->_offset : '' );
    }

}
