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
namespace O2System\O2DB\Drivers\PDO\Subdrivers\ODBC;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Driver extends \O2System\O2DB
{

    /**
     * Sub-driver
     *
     * @var    string
     */
    public $subdriver = 'odbc';

    /**
     * Database schema
     *
     * @var    string
     */
    public $schema = 'public';

    // --------------------------------------------------------------------

    /**
     * Identifier escape character
     *
     * Must be empty for ODBC.
     *
     * @var    string
     */
    protected $_escape_char = '';

    /**
     * ESCAPE statement string
     *
     * @var    string
     */
    protected $_like_escape_str = " {escape '%s'} ";

    /**
     * ORDER BY random keyword
     *
     * @var    array
     */
    protected $_random_keyword = array( 'RND()', 'RND(%d)' );

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * Builds the DSN if not already set.
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

        if( empty( $this->dsn ) )
        {
            $this->dsn = 'odbc:';

            // Pre-defined DSN
            if( empty( $this->hostname ) && empty( $this->HOSTNAME ) && empty( $this->port ) && empty( $this->PORT ) )
            {
                if( isset( $this->DSN ) )
                {
                    $this->dsn .= 'DSN=' . $this->DSN;
                }
                elseif( ! empty( $this->database ) )
                {
                    $this->dsn .= 'DSN=' . $this->database;
                }

                return;
            }

            // If the DSN is not pre-configured - try to build an IBM DB2 connection string
            $this->dsn .= 'DRIVER=' . ( isset( $this->DRIVER ) ? '{' . $this->DRIVER . '}' : '{IBM DB2 ODBC DRIVER}' ) . ';';

            if( isset( $this->DATABASE ) )
            {
                $this->dsn .= 'DATABASE=' . $this->DATABASE . ';';
            }
            elseif( ! empty( $this->database ) )
            {
                $this->dsn .= 'DATABASE=' . $this->database . ';';
            }

            if( isset( $this->HOSTNAME ) )
            {
                $this->dsn .= 'HOSTNAME=' . $this->HOSTNAME . ';';
            }
            else
            {
                $this->dsn .= 'HOSTNAME=' . ( empty( $this->hostname ) ? '127.0.0.1;' : $this->hostname . ';' );
            }

            if( isset( $this->PORT ) )
            {
                $this->dsn .= 'PORT=' . $this->port . ';';
            }
            elseif( ! empty( $this->port ) )
            {
                $this->dsn .= ';PORT=' . $this->port . ';';
            }

            $this->dsn .= 'PROTOCOL=' . ( isset( $this->PROTOCOL ) ? $this->PROTOCOL . ';' : 'TCPIP;' );
        }
    }

    // --------------------------------------------------------------------

    /**
     * Show table query
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
        $sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" . $this->schema . "'";

        if( $prefix_limit !== FALSE && $this->db_prefix !== '' )
        {
            return $sql . " && table_name LIKE '" . $this->escape_like_str( $this->db_prefix ) . "%' "
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
        return 'SELECT column_name FROM information_schema.columns WHERE table_name = ' . $this->escape( $table );
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
     * @param    string    the table name
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
        return preg_replace( '/(^\SELECT (DISTINCT)?)/i', '\\1 TOP ' . $this->qb_limit . ' ', $sql );
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/PDO/subdrivers/ODBC/Driver.php */
