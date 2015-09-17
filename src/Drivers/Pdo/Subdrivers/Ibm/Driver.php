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
namespace O2System\O2DB\Drivers\PDO\Subdrivers\Ibm;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Driver extends \O2System\O2DB
{

    /**
     * Sub-driver
     *
     * @var    string
     */
    public $subdriver = 'ibm';

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
            $this->dsn = 'ibm:';

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
        $sql = 'SELECT "colname" AS "name", "typename" AS "type", "default" AS "default", "length" AS "max_length",
				CASE "keyseq" WHEN NULL THEN 0 ELSE 1 END AS "primary_key"
			FROM "syscat"."columns"
			WHERE LOWER("tabschema") = ' . $this->escape( strtolower( $this->database ) ) . '
				&& LOWER("tabname") = ' . $this->escape( strtolower( $table ) ) . '
			ORDER BY "colno"';

        return ( ( $query = $this->query( $sql ) ) !== FALSE )
            ? $query->result_object()
            : FALSE;
    }

    // --------------------------------------------------------------------

    /**
     * Show table query
     *
     * Generates a platform-specific query string so that the table names can be fetched
     *
     * @access public
     *
     * @param    bool $prefix_limit
     *
     * @return    string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = 'SELECT "tabname" FROM "syscat"."tables"
			WHERE "type" = \'T\' && LOWER("tabschema") = ' . $this->escape( strtolower( $this->database ) );

        if( $prefix_limit === TRUE && $this->db_prefix !== '' )
        {
            $sql .= ' && "tabname" LIKE \'' . $this->escape_like_str( $this->db_prefix ) . "%' "
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
     * @return    array
     */
    protected function _list_columns( $table = '' )
    {
        return 'SELECT "colname" FROM "syscat"."columns"
			WHERE LOWER("tabschema") = ' . $this->escape( strtolower( $this->database ) ) . '
				&& LOWER("tabname") = ' . $this->escape( strtolower( $table ) );
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
        $sql .= ' FETCH FIRST ' . ( $this->qb_limit + $this->qb_offset ) . ' ROWS ONLY';

        return ( $this->qb_offset )
            ? 'SELECT * FROM (' . $sql . ') WHERE rownum > ' . $this->qb_offset
            : $sql;
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/PDO/subdrivers/Ibm/Driver.php */
