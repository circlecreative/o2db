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
namespace O2System\O2DB\Drivers\PDO\Subdrivers\Firebird;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );


class Driver extends \O2System\O2DB
{

    /**
     * Sub-driver
     *
     * @var    string
     */
    public $subdriver = 'firebird';

    // --------------------------------------------------------------------

    /**
     * ORDER BY random keyword
     *
     * @var    array
     */
    protected $_random_keyword = array( 'RAND()', 'RAND()' );

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
            $this->dsn = 'firebird:';

            if( ! empty( $this->database ) )
            {
                $this->dsn .= 'dbname=' . $this->database;
            }
            elseif( ! empty( $this->hostname ) )
            {
                $this->dsn .= 'dbname=' . $this->hostname;
            }

            empty( $this->charset ) OR $this->dsn .= ';charset=' . $this->charset;
            empty( $this->role ) OR $this->dsn .= ';role=' . $this->role;
        }
        elseif( ! empty( $this->charset ) && strpos( $this->dsn, 'charset=', 9 ) === FALSE )
        {
            $this->dsn .= ';charset=' . $this->charset;
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
        $sql = 'SELECT "rfields"."RDB$FIELD_NAME" AS "name",
				CASE "fields"."RDB$FIELD_TYPE"
					WHEN 7 THEN \'SMALLINT\'
					WHEN 8 THEN \'INTEGER\'
					WHEN 9 THEN \'QUAD\'
					WHEN 10 THEN \'FLOAT\'
					WHEN 11 THEN \'DFLOAT\'
					WHEN 12 THEN \'DATE\'
					WHEN 13 THEN \'TIME\'
					WHEN 14 THEN \'CHAR\'
					WHEN 16 THEN \'INT64\'
					WHEN 27 THEN \'DOUBLE\'
					WHEN 35 THEN \'TIMESTAMP\'
					WHEN 37 THEN \'VARCHAR\'
					WHEN 40 THEN \'CSTRING\'
					WHEN 261 THEN \'BLOB\'
					ELSE NULL
				END AS "type",
				"fields"."RDB$FIELD_LENGTH" AS "max_length",
				"rfields"."RDB$DEFAULT_VALUE" AS "default"
			FROM "RDB$RELATION_FIELDS" "rfields"
				JOIN "RDB$FIELDS" "fields" ON "rfields"."RDB$FIELD_SOURCE" = "fields"."RDB$FIELD_NAME"
			WHERE "rfields"."RDB$RELATION_NAME" = ' . $this->escape( $table ) . '
			ORDER BY "rfields"."RDB$FIELD_POSITION"';

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
     * @access protected
     *
     * @param    bool $prefix_limit
     *
     * @return    string
     */
    protected function _list_tables( $prefix_limit = FALSE )
    {
        $sql = 'SELECT "RDB$RELATION_NAME" FROM "RDB$RELATIONS" WHERE "RDB$RELATION_NAME" NOT LIKE \'RDB$%\' && "RDB$RELATION_NAME" NOT LIKE \'MON$%\'';

        if( $prefix_limit === TRUE && $this->db_prefix !== '' )
        {
            return $sql . ' && "RDB$RELATION_NAME" LIKE \'' . $this->escape_like_str( $this->db_prefix ) . "%' "
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
        return 'SELECT "RDB$FIELD_NAME" FROM "RDB$RELATION_FIELDS" WHERE "RDB$RELATION_NAME" = ' . $this->escape( $table );
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
        // Limit clause depends on if Interbase or Firebird
        if( stripos( $this->version(), 'firebird' ) !== FALSE )
        {
            $select = 'FIRST ' . $this->qb_limit
                      . ( $this->qb_offset > 0 ? ' SKIP ' . $this->qb_offset : '' );
        }
        else
        {
            $select = 'ROWS '
                      . ( $this->qb_offset > 0 ? $this->qb_offset . ' TO ' . ( $this->qb_limit + $this->qb_offset ) : $this->qb_limit );
        }

        return preg_replace( '`SELECT`i', 'SELECT ' . $select, $sql );
    }

}

/* End of file Driver.php */
/* Location: ./o2system/libraries/database/drivers/PDO/subdrivers/Firebird/Driver.php */
