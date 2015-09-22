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

namespace O2System\O2DB\Drivers\Pdo\Mysql;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Driver as DriverInterface;

/**
 * PDO MySQL Database Driver
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
    public $sub_driver = 'mysql';

    /**
     * Compression flag
     *
     * @access  compress
     * @type    bool
     */
    public $compress = FALSE;

    /**
     * Strict ON flag
     *
     * Whether we're running in strict SQL mode.
     *
     * @access  public
     * @type    bool
     */
    public $stricton = FALSE;

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * @access  public
     * @type    string
     */
    protected $_escape_character = '`';

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
            $this->dsn = 'mysql:host=' . ( empty( $this->hostname ) ? '127.0.0.1' : $this->hostname );

            empty( $this->port ) OR $this->dsn .= ';port=' . $this->port;
            empty( $this->database ) OR $this->dsn .= ';dbname=' . $this->database;
            empty( $this->charset ) OR $this->dsn .= ';charset=' . $this->charset;
        }
        elseif( ! empty( $this->charset ) && strpos( $this->dsn, 'charset=', 6 ) === FALSE && is_php( '5.3.6' ) )
        {
            $this->dsn .= ';charset=' . $this->charset;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Database connection
     *
     * @todo    SSL support
     *
     * @param   bool $persistent
     *
     * @access  public
     * @return  object
     */
    public function connect( $persistent = FALSE )
    {
        /* Prior to PHP 5.3.6, even if the charset was supplied in the DSN
         * on connect - it was ignored. This is a work-around for the issue.
         *
         * Reference: http://www.php.net/manual/en/ref.pdo-mysql.connection.php
         */
        if( ! is_php( '5.3.6' ) && ! empty( $this->charset ) )
        {
            $this->options[ PDO::MYSQL_ATTR_INIT_COMMAND ] = 'SET NAMES ' . $this->charset
                                                             . ( empty( $this->collate ) ? '' : ' COLLATE ' . $this->collate );
        }

        if( $this->stricton )
        {
            if( empty( $this->options[ PDO::MYSQL_ATTR_INIT_COMMAND ] ) )
            {
                $this->options[ PDO::MYSQL_ATTR_INIT_COMMAND ] = 'SET SESSION sql_mode="STRICT_ALL_TABLES"';
            }
            else
            {
                $this->options[ PDO::MYSQL_ATTR_INIT_COMMAND ] .= ', @@session.sql_mode = "STRICT_ALL_TABLES"';
            }
        }

        if( $this->compress === TRUE )
        {
            $this->options[ PDO::MYSQL_ATTR_COMPRESS ] = TRUE;
        }

        return parent::connect( $persistent );
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
        if( ( $query = $this->query( 'SHOW COLUMNS FROM ' . $this->protect_identifiers( $table, TRUE, NULL, FALSE ) ) ) === FALSE )
        {
            return FALSE;
        }
        $query = $query->result_object();

        $data = array();
        for( $i = 0, $c = count( $query ); $i < $c; $i++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $query[ $i ]->Field;

            sscanf( $query[ $i ]->Type, '%[a-z](%d)',
                    $data[ $i ]->type,
                    $data[ $i ]->max_length
            );

            $data[ $i ]->default = $query[ $i ]->Default;
            $data[ $i ]->primary_key = (int)( $query[ $i ]->Key === 'PRI' );
        }

        return $data;
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
        $sql = 'SHOW TABLES';

        if( $prefix_limit === TRUE && $this->prefix_table !== '' )
        {
            return $sql . " LIKE '" . $this->escape_like_string( $this->prefix_table ) . "%'";
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
        return 'SHOW COLUMNS FROM ' . $this->protect_identifiers( $table, TRUE, NULL, FALSE );
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
        return 'TRUNCATE ' . $table;
    }

    // --------------------------------------------------------------------

    /**
     * FROM tables
     *
     * Groups tables in FROM clauses if needed, so there is no confusion
     * about operator precedence.
     *
     * @access  protected
     * @return  string
     */
    protected function _from_tables()
    {
        if( ! empty( $this->_join ) && count( $this->_from ) > 1 )
        {
            return '(' . implode( ', ', $this->_from ) . ')';
        }

        return implode( ', ', $this->_from );
    }

}
