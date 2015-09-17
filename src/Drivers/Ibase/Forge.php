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
namespace O2System\O2DB\Drivers\iBase;
defined( 'BASEPATH' ) OR exit( 'No direct script access allowed' );

/**
 * Database Driver Class
 *
 * @package        O2System
 * @subpackage     Drivers
 * @category       Database
 * @author         Steeven Andrian Salim
 * @link           http://o2system.center/framework/user-guide/libraries/database.htm
 */
class Forge extends \O2System\O2DB\Interfaces\Forge
{

    /**
     * CREATE TABLE IF statement
     *
     * @var    string
     */
    protected $_create_table_if = FALSE;

    /**
     * RENAME TABLE statement
     *
     * @var    string
     */
    protected $_rename_table = FALSE;

    /**
     * DROP TABLE IF statement
     *
     * @var    string
     */
    protected $_drop_table_if = FALSE;

    /**
     * UNSIGNED support
     *
     * @var    array
     */
    protected $_unsigned = array(
        'SMALLINT' => 'INTEGER',
        'INTEGER'  => 'INT64',
        'FLOAT'    => 'DOUBLE PRECISION'
    );

    /**
     * NULL value representation in CREATE/ALTER TABLE statements
     *
     * @var    string
     */
    protected $_null = 'NULL';

    // --------------------------------------------------------------------

    /**
     * Create database
     *
     * @access public
     *
     * @param    string $db_name
     *
     * @return    string
     */
    public function create_database( $db_name )
    {
        // Firebird databases are flat files, so a path is required

        // Hostname is needed for remote access
        empty( $this->db->hostname ) OR $db_name = $this->hostname . ':' . $db_name;

        return parent::create_database( '"' . $db_name . '"' );
    }

    // --------------------------------------------------------------------

    /**
     * Drop database
     *
     * @access public
     *
     * @param    string $db_name (ignored)
     *
     * @return    bool
     */
    public function drop_database( $db_name = '' )
    {
        if( ! ibase_drop_db( $this->conn_id ) )
        {
            return ( $this->db->db_debug ) ? $this->db->display_error( 'db_unable_to_drop' ) : FALSE;
        }
        elseif( ! empty( $this->db->data_cache[ 'db_names' ] ) )
        {
            $key = array_search( strtolower( $this->db->database ), array_map( 'strtolower', $this->db->data_cache[ 'db_names' ] ), TRUE );
            if( $key !== FALSE )
            {
                unset( $this->db->data_cache[ 'db_names' ][ $key ] );
            }
        }

        return TRUE;
    }

    // --------------------------------------------------------------------

    /**
     * ALTER TABLE
     *
     * @access protected
     *
     * @param    string $alter_type ALTER type
     * @param    string $table      Table name
     * @param    mixed  $field      Column definition
     *
     * @return    string|string[]
     */
    protected function _alter_table( $alter_type, $table, $field )
    {
        if( in_array( $alter_type, array( 'DROP', 'ADD' ), TRUE ) )
        {
            return parent::_alter_table( $alter_type, $table, $field );
        }

        $sql = 'ALTER TABLE ' . $this->db->escape_identifiers( $table );
        $sqls = array();
        for( $i = 0, $c = count( $field ); $i < $c; $i++ )
        {
            if( $field[ $i ][ '_literal' ] !== FALSE )
            {
                return FALSE;
            }

            if( isset( $field[ $i ][ 'type' ] ) )
            {
                $sqls[ ] = $sql . ' ALTER COLUMN ' . $this->db->escape_identififers( $field[ $i ][ 'name' ] )
                           . ' TYPE ' . $field[ $i ][ 'type' ] . $field[ $i ][ 'length' ];
            }

            if( ! empty( $field[ $i ][ 'default' ] ) )
            {
                $sqls[ ] = $sql . ' ALTER COLUMN ' . $this->db->escape_identifiers( $field[ $i ][ 'name' ] )
                           . ' SET DEFAULT ' . $field[ $i ][ 'default' ];
            }

            if( isset( $field[ $i ][ 'null' ] ) )
            {
                $sqls[ ] = 'UPDATE "RDB$RELATION_FIELDS" SET "RDB$NULL_FLAG" = '
                           . ( $field[ $i ][ 'null' ] === TRUE ? 'NULL' : '1' )
                           . ' WHERE "RDB$FIELD_NAME" = ' . $this->db->escape( $field[ $i ][ 'name' ] )
                           . ' && "RDB$RELATION_NAME" = ' . $this->db->escape( $table );
            }

            if( ! empty( $field[ $i ][ 'new_name' ] ) )
            {
                $sqls[ ] = $sql . ' ALTER COLUMN ' . $this->db->escape_identifiers( $field[ $i ][ 'name' ] )
                           . ' TO ' . $this->db->escape_identifiers( $field[ $i ][ 'new_name' ] );
            }
        }

        return $sqls;
    }

    // --------------------------------------------------------------------

    /**
     * Process column
     *
     * @access protected
     *
     * @param    array $field
     *
     * @return    string
     */
    protected function _process_column( $field )
    {
        return $this->db->escape_identifiers( $field[ 'name' ] )
               . ' ' . $field[ 'type' ] . $field[ 'length' ]
               . $field[ 'null' ]
               . $field[ 'unique' ]
               . $field[ 'default' ];
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute TYPE
     *
     * Performs a data type mapping between different databases.
     *
     * @access protected
     *
     * @param    array &$attributes
     *
     * @return    void
     */
    protected function _attr_type( &$attributes )
    {
        switch( strtoupper( $attributes[ 'TYPE' ] ) )
        {
            case 'TINYINT':
                $attributes[ 'TYPE' ] = 'SMALLINT';
                $attributes[ 'UNSIGNED' ] = FALSE;

                return;
            case 'MEDIUMINT':
                $attributes[ 'TYPE' ] = 'INTEGER';
                $attributes[ 'UNSIGNED' ] = FALSE;

                return;
            case 'INT':
                $attributes[ 'TYPE' ] = 'INTEGER';

                return;
            case 'BIGINT':
                $attributes[ 'TYPE' ] = 'INT64';

                return;
            default:
                return;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Field attribute AUTO_INCREMENT
     *
     * @access protected
     *
     * @param    array &$attributes
     * @param    array &$field
     *
     * @return    void
     */
    protected function _attr_auto_increment( &$attributes, &$field )
    {
        // Not supported
    }

}

/* End of file Forge.php */
/* Location: ./o2system/libraries/database/drivers/iBase/Forge.php */
