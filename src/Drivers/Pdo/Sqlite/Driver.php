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

namespace O2System\O2DB\Drivers\Pdo\Sqlite;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * PDO SQLite Database Driver
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
    public $sub_driver = 'sqlite';

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @access  protected
     * @type    array
     */
    protected $_random_keywords = ' RANDOM()';

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Builds the DSN if not already set.
     *
     * @param   array   $params
     *
     * @access  public
     */
    public function __construct( $params )
    {
        parent::__construct( $params );

        if( empty( $this->dsn ) )
        {
            $this->dsn = 'sqlite:';

            if( empty( $this->database ) && empty( $this->hostname ) )
            {
                $this->database = ':memory:';
            }

            $this->database = empty( $this->database ) ? $this->hostname : $this->database;
        }
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
            $data[ $i ]->default = $query[ $i ][ 'default_value' ];
            $data[ $i ]->primary_key = isset( $query[ $i ][ 'pk' ] ) ? (int)$query[ $i ][ 'pk' ] : 0;
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
        $sql = 'SELECT "NAME" FROM "SQLITE_MASTER" WHERE "TYPE" = \'table\'';

        if( $prefix_limit === TRUE && $this->prefix_table !== '' )
        {
            return $sql . ' && "NAME" LIKE \'' . $this->escape_like_string( $this->prefix_table ) . "%' "
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
     * @param   string  $table
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
     * @param   string $table  Table name
     * @param   array  $keys   INSERT keys
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
     * @param   string  $table
     *
     * @access  protected
     * @return  string
     */
    protected function _truncate( $table )
    {
        return 'DELETE FROM ' . $table;
    }

}
